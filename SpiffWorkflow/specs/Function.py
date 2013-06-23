import traceback

from SpiffWorkflow.specs.TaskSpec import TaskSpec, Task


class Function(TaskSpec):
    def __init__(self, parent, name, function, args=None, **kwargs):
        assert function is not None
        assert args is None or isinstance(args, (list, tuple))

        super(Function, self).__init__(parent, name, **kwargs)
        self.function = function
        self.args = args if args else []

    def _try_fire(self, my_task):
        try:
            self.function(*(tuple(self.args) + tuple([my_task])))
        except Exception:
            my_task.fail(failure_message=traceback.format_exc())
            return False
        return True

    def _on_complete_before_hook(self, my_task):
        if my_task.state == Task.FAILED:
            return False
        if not self._try_fire(my_task):
            return False
        else:
            return True

    def serialize(self, serializer):
        return serializer._serialize_function(self)

    @classmethod
    def deserialize(cls, serializer, wf_spec, s_state):
        return serializer._deserialize_function(wf_spec, s_state)

