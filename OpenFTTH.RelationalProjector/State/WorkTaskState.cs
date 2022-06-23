using OpenFTTH.Work.API.Model;
using OpenFTTH.Work.Business.Events;
using System;

namespace OpenFTTH.RelationalProjector.State
{
    /// <summary>
    /// Work task state needed by projection logic
    /// </summary>
    public class WorkTaskState
    {
        public Guid Id { get; set; }
        public string Status { get; set; }

        public static WorkTaskState Create(WorkTaskCreated workTaskCreated)
        {
            return new WorkTaskState()
            {
                Id = workTaskCreated.WorkTaskId.Value,
                Status = workTaskCreated.WorkTask.Status
            };
        }
    }
}
