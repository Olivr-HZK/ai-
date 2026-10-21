#!/usr/bin/env python3
"""Keep a local-only web service reachable through a Quick Tunnel; never touches DNS."""
import json,re,subprocess,sys,time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2];DATA=ROOT/'data/creative_platform';DATA.mkdir(parents=True,exist_ok=True)
def main():
 command=['/opt/homebrew/bin/cloudflared','tunnel','--url','http://127.0.0.1:8877','--no-autoupdate','--protocol','http2']
 child=subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,bufsize=1)
 with (DATA/'tunnel.log').open('a') as log:
  for line in child.stdout:
   log.write(line);log.flush()
   found=re.search(r'https://[a-z0-9-]+\.trycloudflare\.com',line)
   if found:
    value={'url':found[0],'temporary':True,'updated_at':time.time(),'domain_bound':False};temp=DATA/'public-endpoint.tmp';temp.write_text(json.dumps(value,indent=2));temp.replace(DATA/'public-endpoint.json')
    access=DATA/'admin-access.txt'
    if access.exists():access.write_text(re.sub(r'公网测试：https://[^\n]+','公网测试：'+found[0],access.read_text()))
    print('Public test URL: '+found[0],flush=True)
 raise SystemExit(child.wait())
if __name__=='__main__':main()
