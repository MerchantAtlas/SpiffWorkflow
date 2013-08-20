
from SpiffWorkflow.specs import Function


class WaitForTrigger(Function):
    """
    This task does nothing until it is triggered, then it is allowed to
    Complete.
    """

    def _try_fire(self, my_task):
        """Call the given function with the given arguments and the task as
        *args
        """
        if my_task.triggered:
            return super(WaitForTrigger, self)._try_fire(my_task)
        else:
            return False


