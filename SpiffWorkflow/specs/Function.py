import traceback

from SpiffWorkflow.specs.TaskSpec import TaskSpec, Task


class Function(TaskSpec):
    """
    When executed, this task calls a given function with given arguments
    and the current task. The function must respond in specific ways:
        1) Return True: Completes the task
        2) Return False: Sets the task to Incomplete, and will retry
        3) Raise Exception: Logs the error message and fails the task
    """

    def __init__(self, parent, name, function, args=None, **kwargs):
        assert function is not None
        assert args is None or isinstance(args, (list, tuple))

        super(Function, self).__init__(parent, name, **kwargs)
        self.function = function
        self.args = args if args else []

    def _try_fire(self, my_task):
        """Call the given function with the given arguments and the task as
        *args
        """
        return self.function(*(tuple(self.args) + tuple([my_task])))

    def _on_complete_before_hook(self, my_task):
        # If this task is already Failed, don't bother running it again.
        if my_task.state == Task.FAILED:
            return False

        try:
            result = self._try_fire(my_task)
            # If the function returns 'False' then it is not done with what
            # ever it is supposed to be doing, so mark it Incomplete.
            if not result:
                my_task.state = Task.INCOMPLETE

            return result
        except Exception:
            # If the function raises an exception, save the error and fail
            my_task.fail(failure_message=traceback.format_exc())
            raise

    def _on_trigger(self, my_task):
        """If this task is triggered, set the triggered attribute and attempt
        to complete the task
        """
        my_task.triggered = True

    def serialize(self, serializer):
        return serializer._serialize_function(self)

    @classmethod
    def deserialize(cls, serializer, wf_spec, s_state):
        return serializer._deserialize_function(wf_spec, s_state)

