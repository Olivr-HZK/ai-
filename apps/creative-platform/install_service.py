#!/usr/bin/env python3
"""Install per-user macOS agents for the portal and optional public test tunnel."""
import argparse,os,plistlib,subprocess,sys
from pathlib import Path
HERE=Path(__file__).resolve().parent;PROJECT=HERE.parents[1];DATA=PROJECT/'data/creative_platform'
def main():
 p=argparse.ArgumentParser();p.add_argument('--tunnel',action='store_true');p.add_argument('--uninstall',action='store_true');a=p.parse_args();agents=Path.home()/'Library/LaunchAgents';agents.mkdir(exist_ok=True);DATA.mkdir(parents=True,exist_ok=True);python=PROJECT/'.venv/bin/python';uid=os.getuid()
 jobs=[('cn.myguru.creative-platform',['/usr/bin/caffeinate','-i',str(python),str(HERE/'server.py'),'--port','8877'],'server')]
 if a.tunnel:jobs.append(('cn.myguru.creative-platform-tunnel',[str(python),str(HERE/'tunnel.py')],'tunnel-agent'))
 for label,args,log in jobs:
  path=agents/(label+'.plist');subprocess.run(['launchctl','bootout',f'gui/{uid}/{label}'],capture_output=True)
  if a.uninstall:
   if path.exists():path.unlink()
   print(label+' stopped');continue
  config={'Label':label,'ProgramArguments':args,'WorkingDirectory':str(PROJECT),'RunAtLoad':True,'KeepAlive':True,'ThrottleInterval':10,'StandardOutPath':str(DATA/(log+'.stdout.log')),'StandardErrorPath':str(DATA/(log+'.stderr.log')),'EnvironmentVariables':{'PYTHONUNBUFFERED':'1','PATH':'/opt/homebrew/bin:/usr/bin:/bin'}}
  with path.open('wb') as f:plistlib.dump(config,f)
  result=subprocess.run(['launchctl','bootstrap',f'gui/{uid}',str(path)],capture_output=True,text=True)
  if result.returncode:raise SystemExit('Service setup failed for '+label+': '+result.stderr)
  print(label+' installed')
if __name__=='__main__':main()
