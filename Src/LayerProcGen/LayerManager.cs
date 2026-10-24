/*
 * Copyright (c) 2024 Rune Skovbo Johansen
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

using Runevision.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Runevision.LayerProcGen {

	/// <summary>
	/// The central LayerManager that takes care of generating the required chunks.
	/// </summary>
	public class LayerManager {

		public static LayerManager instance;
		static readonly AsyncLocal<GenerationPriority?> requestPriority = new();
		public static GenerationPriority CurrentRequestPriority => requestPriority.Value ?? GenerationPriority.Foreground;

		public bool useParallelThreads { get; private set; }
        int activePriority;
        internal bool UseParallelForCurrentRequest => useParallelThreads
            && Volatile.Read(ref activePriority) == (int)GenerationPriority.Foreground;
        long admittedRequests, completedRequests, interruptedRequests, requestTicks, foregroundWaitTicks;
        int runningRequests;
        public GenerationSchedulerSnapshot SchedulerSnapshot {
            get {
                lock (topDependencies) {
                    return new GenerationSchedulerSnapshot(
                        topDependencies.Count(d => d.changed && d.Priority == GenerationPriority.Foreground),
                        topDependencies.Count(d => d.changed && d.Priority == GenerationPriority.Speculative),
                        Volatile.Read(ref runningRequests), Interlocked.Read(ref admittedRequests),
                        Interlocked.Read(ref completedRequests), Interlocked.Read(ref interruptedRequests),
                        Interlocked.Read(ref requestTicks) * 1000.0 / System.Diagnostics.Stopwatch.Frequency,
                        Interlocked.Read(ref foregroundWaitTicks) * 1000.0 / System.Diagnostics.Stopwatch.Frequency,
                        Volatile.Read(ref runningRequests) != 0
                            && Volatile.Read(ref activePriority) == (int)GenerationPriority.Foreground);
                }
            }
        }
        internal bool HasPendingForeground() {
            lock (topDependencies)
                return topDependencies.Any(d => d.changed && d.Priority == GenerationPriority.Foreground);
        }
        internal bool ShouldYieldSpeculative => !SpeculativeAdmissionEnabled || HasPendingForeground();
		public bool isGeneratingInBackground { get; private set; }
		public bool building { get { return isGeneratingInBackground || !MainThreadActionQueue.idle; } }
		// Monotonically increases after each top-level dependency request has been
		// completely processed. Interrupted requests never advance this marker.
		// Consumers can use this to stage large requests
		// without guessing whether the background worker has observed a change.
		int completedGenerationPasses;
		readonly System.Threading.Tasks.TaskCompletionSource<Exception> failure = new(System.Threading.Tasks.TaskCreationOptions.RunContinuationsAsynchronously);
		public System.Threading.Tasks.Task<Exception> Failure => failure.Task;
		public int CompletedGenerationPasses { get { return Volatile.Read(ref completedGenerationPasses); } }

		public event Action abort;
		private volatile bool stopRequested;
		public bool aborting { get { return stopRequested; } private set { stopRequested = value; } }

		public List<TopLayerDependency> topDependencies { get; } = new List<TopLayerDependency>();
		/// <summary>Admission switch only; an already running plugin yields at its next cancellation check.</summary>
		volatile bool speculativeAdmissionEnabled = true;
		public bool SpeculativeAdmissionEnabled { get => speculativeAdmissionEnabled; set => speculativeAdmissionEnabled = value; }
		public void AddTopDependency(TopLayerDependency d) {
			lock (topDependencies) {
				if (topDependencies.Contains(d)) return;
				if (d.Priority == GenerationPriority.Speculative && topDependencies.Any(other => other.Priority == GenerationPriority.Speculative))
					throw new InvalidOperationException("Reuse the existing speculative dependency; only one root request may be retained.");
				topDependencies.Add(d);
			}
		}
		public void RemoveTopDependency(TopLayerDependency d) {
			lock (topDependencies) topDependencies.Remove(d);
		}

		Thread layerUpdateThread;
		readonly System.Collections.Concurrent.ConcurrentQueue<ChunkLevelData> deferredProviderReleases = new();

		internal void DeferProviderRelease(ChunkLevelData data) {
			deferredProviderReleases.Enqueue(data);
		}

		void ReleaseJoinedProviders() {
			while (deferredProviderReleases.TryDequeue(out var data)) {
				foreach (var provider in data.providers)
					provider.chunk.DecrementUserCountOfLevel(provider.level);
				ObjectPool<ChunkLevelData>.GlobalReturn(ref data);
			}
		}

		public LayerManager(bool useParallelThreads) {
			instance = this;

			this.useParallelThreads = useParallelThreads;

			AppDomain currentDomain = AppDomain.CurrentDomain;
			currentDomain.UnhandledException += MyExceptionHandler;

			aborting = false;
			layerUpdateThread = new Thread(LayerUpdateThread);
			layerUpdateThread.Priority = ThreadPriority.Lowest;
			layerUpdateThread.Start();
		}

		static void MyExceptionHandler(object sender, UnhandledExceptionEventArgs args) {
			Exception e = (Exception)args.ExceptionObject;
			Logg.LogError($"Thread: {e.Message}\n{e.StackTrace}");
		}

		/// <summary>How long Unload waits for in-flight generation before abandoning it.</summary>
		public static int unloadTimeoutMs = 5000;

		/// <summary>Stop admitting work and join active jobs without resetting layer data.</summary>
		public void Quiesce() {
			aborting = true;
			if (Thread.CurrentThread == layerUpdateThread)
				throw new InvalidOperationException("Generation cannot join its own worker.");
			layerUpdateThread.Join();
			isGeneratingInBackground = false;
			if (failure.Task.IsCompletedSuccessfully)
				throw new InvalidOperationException("Generation worker failed; final save is unsafe.", failure.Task.Result);
		}

		public void Unload() {
			aborting = true;

			// Bounded shutdown. This used to be an unbounded Join followed by a tight
			// do/while (building) spin: the Unity coroutine's "yield return null" was dropped
			// when this was ported, so it also burned a core while waiting. That stayed
			// harmless as long as generation drained in a few seconds (the legacy heightmap
			// path did), but with 3D density terrain it does not drain and Ctrl+C looked like
			// a hang -- the process kept running with the shutdown callback stuck here.
			// Callers should cancel outstanding generation before calling this; the timeout is
			// the backstop for when they cannot.
			if (!layerUpdateThread.Join(unloadTimeoutMs))
				Logg.LogWarning($"LayerManager.Unload: layer update thread did not stop within {unloadTimeoutMs}ms; abandoning it.");

			long deadline = Environment.TickCount64 + unloadTimeoutMs;
			while (true) {
				lock (topDependencies) {
					for (int i = 0; i < topDependencies.Count; i++) {
						topDependencies[i].isActive = false;
					}
				}
				if (!building)
					break;
				if (Environment.TickCount64 >= deadline) {
					Logg.LogWarning($"LayerManager.Unload: generation still in flight after {unloadTimeoutMs}ms; abandoning it.");
					break;
				}
				Thread.Sleep(1);
			}

			AbstractDataLayer.ResetInstances();
			instance = null;
		}

		public void OnDestroy() {
			aborting = true;
			layerUpdateThread.Join();
			abort?.Invoke();
		}

        void LayerUpdateThread() {
            while (!aborting) {
                TopLayerDependency dependency;
                lock (topDependencies) {
                    // Snapshot selection only. Never hold the collection lock while
                    // a plugin runs, provider jobs join, or old roots are released.
                    dependency = topDependencies.Where(d => d.changed && (d.Priority == GenerationPriority.Foreground
                        || !d.isActive || SpeculativeAdmissionEnabled))
                        .OrderBy(d => d.Priority).FirstOrDefault();
                }
                if (dependency == null) {
                    isGeneratingInBackground = false;
                    Thread.Sleep(5);
                    continue;
                }
                WorkTracker.AddWorkEstimated(100, dependency.layer.GetType());
                var ph = SimpleProfiler.Begin("LayerManager Process", 1);
                isGeneratingInBackground = true;
                Volatile.Write(ref activePriority, (int)dependency.Priority);
                requestPriority.Value = dependency.Priority;
                Volatile.Write(ref runningRequests, 1);
                Interlocked.Increment(ref admittedRequests);
                long started = System.Diagnostics.Stopwatch.GetTimestamp();
                try {
                    bool completed;
                    int completedRevision;
                    try { completed = dependency.abstractLayer.ProcessTopDependency(dependency, out completedRevision); }
                    finally { ReleaseJoinedProviders(); }
                    if (completed) {
                        dependency.MarkCompleted(completedRevision);
                        Interlocked.Increment(ref completedRequests);
                        Interlocked.Increment(ref completedGenerationPasses);
                        WorkTracker.WorkIsKnown(dependency.layer.GetType());
                        lock (topDependencies) {
                            if (!dependency.isActive && !dependency.changed) topDependencies.Remove(dependency);
                        }
                    } else {
                        // Preemption leaves prior root ownership intact and retries
                        // after newly arrived foreground work. It is not a completed pass.
                        dependency.RetryGeneration();
                        Interlocked.Increment(ref interruptedRequests);
                    }
                } catch (Exception error) {
                    Interlocked.Increment(ref interruptedRequests);
                    var errors = error is AggregateException aggregate
                        ? aggregate.Flatten().InnerExceptions.ToArray() : new[] { error };
                    if (errors.All(e => e is OperationCanceledException || e is RetryableGenerationException)) {
                        dependency.RetryGeneration();
                        if (!aborting) Thread.Sleep(50);
                    } else {
                        failure.TrySetResult(error);
                        aborting = true;
                    }
                } finally {
                    long elapsed = System.Diagnostics.Stopwatch.GetTimestamp() - started;
                    Interlocked.Add(ref requestTicks, elapsed);
                    // While an uninterruptible speculative plugin finishes, this is
                    // an upper bound on foreground blocking, not exact queue latency.
                    if (dependency.Priority == GenerationPriority.Speculative && HasPendingForeground())
                        Interlocked.Add(ref foregroundWaitTicks, elapsed);
                    Volatile.Write(ref runningRequests, 0);
                    Volatile.Write(ref activePriority, (int)GenerationPriority.Foreground);
                    requestPriority.Value = null;
                    SimpleProfiler.End(ph);
                }
            }
            isGeneratingInBackground = false;
        }
	}

	/// <summary>Root admission counts; active dependency halos are not individual root requests.</summary>
    public readonly record struct GenerationSchedulerSnapshot(int PendingForeground, int PendingSpeculative,
        int Running, long Admitted, long Completed, long Interrupted, double ExecutionMs,
        double ForegroundBlockingUpperBoundMs, bool RunningForeground);

    /// <summary>Only throw when a failed stage has discarded all uncommitted mutations.</summary>
	public sealed class RetryableGenerationException : Exception {
		public RetryableGenerationException(string message, Exception inner) : base(message, inner) { }
	}

}
