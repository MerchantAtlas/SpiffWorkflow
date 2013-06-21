# Copyright (C) 2013 Merchant Atlas Inc.
# Copyright (C) 2007 Samuel Abels
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

from SpiffWorkflow.Task import Task
from SpiffWorkflow.exceptions import WorkflowException
from SpiffWorkflow.specs.TaskSpec import TaskSpec
import SpiffWorkflow


class SubWorkflow2(TaskSpec):
    """
    A SubWorkflow is a task that wraps a WorkflowSpec, such that you can
    re-use it in multiple places as if it were a task.
    If more than one input is connected, the task performs an implicit
    multi merge.
    If more than one output is connected, the task performs an implicit
    parallel split.
    """
    def __init__(self,
                 parent,
                 name,
                 workflow_spec,
                 in_assign=None,
                 out_assign=None,
                 **kwargs):
        """
        Constructor.

        :type  parent: WorkflowSpec
        :param parent: A reference to the parent (usually a WorkflowSpec).
        :type  name: str
        :param name: The name of the task spec.
        :type  workflow_spec: WorkflowSpec
        :param workflow_spec: A WorkflowSpec to generate a Workflow from.
        :type  in_assign: list(str)
        :param in_assign: A list of Assign objects for fields to carry in
        :type  out_assign: list(str)
        :param out_assign: A list of Assign objects for fields to carry back.
        :type  kwargs: dict
        :param kwargs: See L{SpiffWorkflow.specs.TaskSpec}.
        """
        assert parent is not None
        assert name is not None
        TaskSpec.__init__(self, parent, name, **kwargs)
        self.in_assign = in_assign is not None and in_assign or []
        # TODO: As of now, out_assign does not filter. Make it work
        self.out_assign = out_assign is not None and out_assign or []
        self.workflow_spec = workflow_spec

    def _predict_hook(self, my_task):
        outputs = [task.task_spec for task in my_task.children]
        for output in self.outputs:
            if output not in outputs:
                outputs.insert(0, output)
        if my_task._is_definite():
            my_task._sync_children(outputs, Task.FUTURE)
        else:
            my_task._sync_children(outputs, my_task.state)

    def _create_subworkflow(self, my_task):
        return SpiffWorkflow.Workflow(self.workflow_spec,
                                      parent=my_task.workflow.outer_workflow)

    def _on_ready_before_hook(self, my_task):
        subworkflow = self._create_subworkflow(my_task)

        # Integrate the tree of the subworkflow into the tree of this workflow.
        for child in subworkflow.task_tree.children:
            my_task.children.insert(0, child)
            child.parent = my_task

        # Find the last child of the subworkflow
        tails = []
        for task in Task.Iterator(subworkflow.task_tree):
            if not task.children:
                tails.append(task)

        if len(tails) > 1:
            raise WorkflowException(
                    my_task,
                    "Subworkflows may finish with only one branch. Merge "
                    "branches together if necessary.")

        # Append this task's outputs to the last child of the subworkflow
        tail = tails[0]
        tail._sync_children(self.outputs, Task.LIKELY)
        tail.task_spec.outputs = self.outputs

        for task in my_task.children:
            if task.task_spec in self.outputs:
                # Set the tail of the subworkflow as the parent of the outputs
                task.parent = tail
                # Remove the non-subworkflow outputs from this task. The
                # subworkflow is now the path to the rest of this workflow.
                my_task.children.remove(task)

        # Remove the SubWorkflow TaskSpec from the inputs of the outputs, and
        # replace it with the tail of the subworkflow
        for output in self.outputs:
            output.inputs.remove(self)
            output.inputs.append(tail.task_spec)

        # Change outputs to only point at the head of the subworkflow
        self.outputs = [c.task_spec for c in my_task.children]
        my_task._set_internal_data(subworkflow=subworkflow)

    def _on_ready_hook(self, my_task):
        # Assign variables, if so requested.
        subworkflow = my_task._get_internal_data('subworkflow')
        for child in subworkflow.task_tree.children:
            for assignment in self.in_assign:
                assignment.assign(my_task, child)

        self._predict(my_task)
        for child in subworkflow.task_tree.children:
            child.task_spec._update_state(child)

    def serialize(self, serializer):
        return serializer._serialize_sub_workflow2(self)

    @classmethod
    def deserialize(cls, serializer, wf_spec, s_state):
        return serializer._deserialize_sub_workflow2(wf_spec, s_state)
