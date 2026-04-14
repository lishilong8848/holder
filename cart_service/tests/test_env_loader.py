import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.env_loader import load_app_env, resolve_dotenv_path


class EnvLoaderTests(unittest.TestCase):
    def test_resolve_dotenv_path_prefers_internal_env_when_frozen(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            exe_dir = Path(temp_dir) / "FeishuCertBot"
            internal_dir = exe_dir / "_internal"
            internal_dir.mkdir(parents=True)
            (internal_dir / ".env").write_text("PORT=58001\n", encoding="utf-8")
            (exe_dir / ".env").write_text("PORT=58002\n", encoding="utf-8")

            with patch.object(os.sys, "frozen", True, create=True), patch.object(
                os.sys, "executable", str(exe_dir / "FeishuCertBot.exe")
            ):
                env_path = resolve_dotenv_path()

            self.assertEqual(env_path, internal_dir / ".env")

    def test_resolve_dotenv_path_falls_back_to_exe_dir_env_when_internal_missing(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            exe_dir = Path(temp_dir) / "FeishuCertBot"
            exe_dir.mkdir(parents=True)
            (exe_dir / ".env").write_text("PORT=58002\n", encoding="utf-8")

            with patch.object(os.sys, "frozen", True, create=True), patch.object(
                os.sys, "executable", str(exe_dir / "FeishuCertBot.exe")
            ):
                env_path = resolve_dotenv_path()

            self.assertEqual(env_path, exe_dir / ".env")

    def test_load_app_env_uses_project_root_env_in_source_mode(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir)
            env_file = project_root / ".env"
            env_file.write_text("UNITTEST_ENV_LOADER=from_env_loader\n", encoding="utf-8")

            original = os.environ.get("UNITTEST_ENV_LOADER")
            os.environ.pop("UNITTEST_ENV_LOADER", None)
            try:
                with patch.object(os.sys, "frozen", False, create=True):
                    loaded_path = load_app_env(project_root=project_root, override=True)
                self.assertEqual(loaded_path, env_file)
                self.assertEqual(os.environ.get("UNITTEST_ENV_LOADER"), "from_env_loader")
            finally:
                if original is None:
                    os.environ.pop("UNITTEST_ENV_LOADER", None)
                else:
                    os.environ["UNITTEST_ENV_LOADER"] = original


if __name__ == "__main__":
    unittest.main()
