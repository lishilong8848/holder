import tempfile
import unittest
from pathlib import Path

from app.task_registry import TASK_STATUS_ENDED, TASK_STATUS_SUCCESS, TaskRegistry


class TaskRegistryTests(unittest.TestCase):
    def test_task_history_persists_and_reloads_latest_snapshot(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            history_path = Path(temp_dir) / "tasks.jsonl"
            registry = TaskRegistry(history_path)

            task = registry.create_task(workflow="certificate", record_id="rec_001", source="ui")
            registry.update_task(task.task_id, current_step="处理中", progress_current=1, progress_total=2)
            registry.finish_task(task.task_id, status=TASK_STATUS_SUCCESS, current_step="完成")

            reloaded = TaskRegistry(history_path)
            loaded_task = reloaded.get_task(task.task_id)

            self.assertIsNotNone(loaded_task)
            self.assertEqual(loaded_task["status"], TASK_STATUS_SUCCESS)
            self.assertEqual(loaded_task["current_step"], "完成")
            self.assertEqual(len(reloaded.list_tasks()), 1)

    def test_reloaded_active_task_is_marked_ended(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            history_path = Path(temp_dir) / "tasks.jsonl"
            registry = TaskRegistry(history_path)

            task = registry.create_task(workflow="certificate", record_id="rec_001", source="ui")

            reloaded = TaskRegistry(history_path)
            loaded_task = reloaded.get_task(task.task_id)

            self.assertIsNotNone(loaded_task)
            self.assertEqual(loaded_task["status"], TASK_STATUS_ENDED)
            self.assertEqual(loaded_task["current_step"], "服务已重启，历史任务按已结束处理")
            self.assertEqual(reloaded.stats()["running"], 0)
            self.assertEqual(reloaded.stats()["ended"], 1)

    def test_events_default_excludes_info(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            history_path = Path(temp_dir) / "tasks.jsonl"
            registry = TaskRegistry(history_path)

            registry.add_event("system", "普通信息", level="info")
            registry.add_event("system", "警告信息", level="warning")
            registry.add_event("system", "错误信息", level="error")

            events = registry.events()

            self.assertEqual([item["message"] for item in events], ["警告信息", "错误信息"])


if __name__ == "__main__":
    unittest.main()
