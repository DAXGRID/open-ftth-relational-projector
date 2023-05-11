namespace OpenFTTH.RelationalProjector.State
{
    public abstract class ObjectState
    {
        public LatestChangeType LatestChangeType { get; set; }

        public ObjectState(LatestChangeType latestChangeType)
        {
            LatestChangeType = latestChangeType;
        }
    }

    public enum LatestChangeType
    {
        NEW = 0,
        UPDATED = 1,
        REMOVED = 2,
    }
}
