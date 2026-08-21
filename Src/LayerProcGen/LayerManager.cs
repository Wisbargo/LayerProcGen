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

		public bool useParallelThreads { get; private set; }
		public bool isGeneratingInBackground { get; private set; }
		public bool building { get { return isGeneratingInBackground || !MainThreadActionQueue.idle; } }
		// Monotonically increases after a top-level dependency request has been
		// completely processed. Consumers can use this to stage large requests
		// without guessing whether the background worker has observed a change.
		int completedGenerationPasses;
		public int CompletedGenerationPasses { get { return Volatile.Read(ref completedGenerationPasses); } }

		public event Action abort;
		public bool aborting { get; private set; }

		public List<TopLayerDependency> topDependencies { get; } = new List<TopLayerDependency>();
		public void AddTopDependency(TopLayerDependency d) {
			if (!topDependencies.Contains(d))
				topDependencies.Add(d);
		}
		public void RemoveTopDependency(TopLayerDependency d) {
			if (topDependencies.Contains(d))
				topDependencies.Remove(d);
		}

		Thread layerUpdateThread;

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
				bool changed = false;
				lock (topDependencies) {
					for (int i = 0; i < topDependencies.Count; i++) {
						TopLayerDependency dep = topDependencies[i];
						if (dep.changed) { // TODO: Occasional nullref here?
							WorkTracker.AddWorkEstimated(100, dep.layer.GetType());
							changed = true;
						}
					}
				}

				if (changed) {
					var ph = SimpleProfiler.Begin("LayerManager Process", 1);

					isGeneratingInBackground = true;
					lock (topDependencies) {
						for (int i = 0; i < topDependencies.Count; i++) {
							TopLayerDependency dep = topDependencies[i];
							// Generate to address changed top dependencies.
							if (dep.changed) {
								dep.abstractLayer.ProcessTopDependency(dep);
								WorkTracker.WorkIsKnown(dep.layer.GetType());

								// Remove inactive top dependencies.
								if (!dep.isActive) {
									RemoveTopDependency(dep);
								}
							}
						}
					}

					SimpleProfiler.End(ph);
					Interlocked.Increment(ref completedGenerationPasses);

					if (SimpleProfiler.AnyCurrent())
						SimpleProfiler.Log();
					if (Logg.ConsoleEnabled) {
						lock (PoolManager.allPools) {
							var sorted = PoolManager.allPools.OrderBy(e => e.ToString());
							foreach (IPool pool in sorted) {
								Console.WriteLine("Pool {0} {1,6} capacity ({2,6} active, {3,6} inactive)",
									pool.ToString().PadRight(50, '.'),
									pool.CountAll, pool.CountActive, pool.CountInactive);
							}
						}
					}
				}
				else {
					isGeneratingInBackground = false;
					if (!aborting)
						Thread.Sleep(5);
				}
			}
		}
	}

}
