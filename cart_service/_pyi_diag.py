from pathlib import Path
import sys
log = Path(r'd:/code/holder/cart_service/_pyi_diag.log')
log.write_text('', encoding='utf-8')
def mark(s):
    with log.open('a', encoding='utf-8') as f:
        f.write(s + '\n')
        f.flush()
mark('before import')
import PyInstaller.__main__ as m
mark('after import')
m.compat.check_requirements()
mark('after compat')
m.check_unsafe_privileges()
mark('after unsafe')
parser = m.generate_parser()
mark('after parser')
args = parser.parse_args(['--log-level','DEBUG','--noconfirm','--clean','--onedir','--name','_pyi_hello','d:/code/holder/cart_service/_pyi_hello.py'])
mark('after parse_args')
import PyInstaller.log
PyInstaller.log.__process_options(parser, args)
mark('after process_options')
try:
    from _pyinstaller_hooks_contrib import __version__ as contrib_hooks_version
except Exception:
    contrib_hooks_version = 'unknown'
mark('after contrib')
mark('about makespec')
spec_file = m.run_makespec(**vars(args))
mark('after makespec:' + str(spec_file))
sys.argv = [spec_file]
mark('about build')
m.run_build(None, spec_file, **vars(args))
mark('after build')
