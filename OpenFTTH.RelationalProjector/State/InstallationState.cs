using OpenFTTH.Installation.Events;
using System;

namespace OpenFTTH.RelationalProjector.State
{
    /// <summary>
    /// Installation state needed by projection logic
    /// </summary>
    public class InstallationState : ObjectState
    {
        public Guid Id { get; set; }
        public string InstallationId { get; set; }
        public Guid? UnitAddressId { get; set; }
        public string Status { get; set; }
        public string LocationRemark { get; set; }

        public InstallationState(LatestChangeType latestChangeType) : base(latestChangeType)
        {
        }

        public static InstallationState Create(InstallationCreated installationCreated)
        {
            var state = new InstallationState(LatestChangeType.NEW)
            {
                Id = installationCreated.Id,
                InstallationId = installationCreated.InstallationId,
                UnitAddressId = installationCreated.UnitAddressId,
                Status = installationCreated.Status,
                LocationRemark = installationCreated.LocationRemark
            };

            return state;
        }

    }
}
