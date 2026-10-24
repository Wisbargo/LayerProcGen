/*
 * Copyright (c) 2024 Rune Skovbo Johansen
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

using Runevision.Common;

namespace Runevision.LayerProcGen {
    public enum GenerationPriority { Foreground, Speculative }

    /// <summary>A foreground view or one bounded speculative root chunk.</summary>
    public class TopLayerDependency {
        readonly object requestGate = new();
        public GenerationPriority Priority { get; }
        public IChunkBasedDataLayer layer => (IChunkBasedDataLayer)abstractLayer;
        public AbstractChunkBasedDataLayer abstractLayer { get; }
        public int level { get; }
        public Point focus { get; private set; }
        public Point size { get; private set; }
        public GridBounds chunkIndices { get; private set; }
        volatile bool requestChanged;
        public bool changed => requestChanged;
        int generationRevision;
        int completedRevision = -1;
        public bool IsSettled => !requestChanged && Volatile.Read(ref completedRevision) == Volatile.Read(ref generationRevision);
        internal ChunkLevelData currentRootUsage;
        volatile bool active;

        public bool isActive {
            get => active;
            set {
                lock (requestGate) {
                    if (active == value) return;
                    if (value) ValidateSpeculativeBounds(focus, size);
                    active = value;
                    requestChanged = true;
                    Interlocked.Increment(ref generationRevision);
                }
                if (value) LayerManager.instance.AddTopDependency(this);
            }
        }

        public TopLayerDependency(AbstractChunkBasedDataLayer layer, Point size, int level,
            GenerationPriority priority = GenerationPriority.Foreground) {
            abstractLayer = layer;
            this.size = size;
            this.level = level;
            Priority = priority;
        }
        public TopLayerDependency(AbstractChunkBasedDataLayer layer, Point size)
            : this(layer, size, layer.GetLevelCount() - 1) { }
        public TopLayerDependency(AbstractChunkBasedDataLayer layer, Point size, GenerationPriority priority)
            : this(layer, size, layer.GetLevelCount() - 1, priority) { }

        void ValidateSpeculativeBounds(Point nextFocus, Point nextSize) {
            if (Priority != GenerationPriority.Speculative) return;
            GridBounds indices = new GridBounds(nextFocus - nextSize / 2, nextSize).GetDivided(layer.chunkSize);
            if (nextSize.x <= 0 || nextSize.y <= 0 || indices.size.x != 1 || indices.size.y != 1)
                throw new ArgumentOutOfRangeException(nameof(nextSize), "Speculative requests must cover exactly one root chunk.");
        }

        public void SetFocus(Point nextFocus) {
            lock (requestGate) {
                if (active && nextFocus == focus) return;
                ValidateSpeculativeBounds(nextFocus, size);
                focus = nextFocus;
                UpdateLayerIndices();
            }
            LayerManager.instance.AddTopDependency(this);
        }
        public void SetSize(Point nextSize) {
            lock (requestGate) {
                if (active && nextSize == size) return;
                ValidateSpeculativeBounds(focus, nextSize);
                size = nextSize;
                UpdateLayerIndices();
            }
            LayerManager.instance.AddTopDependency(this);
        }
        public void SetPadding(Point padding) => SetSize(padding * 2);

        void UpdateLayerIndices() {
            GridBounds oldIndices = chunkIndices;
            chunkIndices = new GridBounds(focus - size / 2, size).GetDivided(layer.chunkSize);
            if (chunkIndices != oldIndices || !active) {
                Interlocked.Increment(ref generationRevision);
                requestChanged = true;
            }
            active = true;
        }
        internal void GetPendingBounds(out GridBounds bounds, out int requestedLevel, out int revision) {
            lock (requestGate) {
                requestChanged = false;
                bounds = new GridBounds(focus - size / 2, size);
                requestedLevel = level;
                revision = Volatile.Read(ref generationRevision);
            }
        }
        internal bool IsGenerationCurrent(int revision) => active && Volatile.Read(ref generationRevision) == revision;
        internal void MarkCompleted(int revision) => Volatile.Write(ref completedRevision, revision);
        internal void RetryGeneration() { requestChanged = true; }
    }
}
