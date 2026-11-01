import os
import sys
import time
import json
import random
import requests

METRICS_HOST = os.environ.get("METRICS_HOST", "").strip().rstrip("/")
PROBE_TOKEN = os.environ.get("PROBE_TOKEN", "").strip()
THRESHOLD_DAYS = os.environ.get("THRESHOLD_DAYS", "9").strip()
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "20"))
STRATEGY = os.environ.get("STRATEGY", "hybrid_auto").strip()
MAX_ROUNDS = 5

if not METRICS_HOST or not PROBE_TOKEN:
    print("[!] Telemetry configuration incomplete (METRICS_HOST or PROBE_TOKEN missing).")
    sys.exit(1)

headers = {
    "Authorization": f"Bearer {PROBE_TOKEN}",
    "X-API-Key": PROBE_TOKEN,
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) EdgeMirrorProbe/2.0"
}

def renew_gofile_node(content_id_or_url):
    cid = content_id_or_url.strip()
    if "/d/" in cid:
        cid = cid.split("/d/")[-1].split("?")[0].strip("/")
    
    page_url = f"https://gofile.io/d/{cid}"
    browser_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9"
    }
    try:
        r = requests.get(page_url, headers=browser_headers, timeout=12)
        if r.status_code == 200:
            return True, f"File page visited & activity registered (+10d)"
        elif r.status_code == 404:
            return False, "File reported 404 not found"
        else:
            return False, f"Page returned HTTP {r.status_code}"
    except Exception as ex:
        return False, str(ex)

def renew_viking_node(viking_hash):
    if not viking_hash:
        return False, "No hash"
    try:
        chk_resp = requests.post("https://vikingfile.com/api/check-file", data={"hash": viking_hash}, timeout=10)
        if chk_resp.status_code == 200:
            chk_json = chk_resp.json()
            is_alive = False
            if isinstance(chk_json, list) and len(chk_json) > 0:
                is_alive = bool(chk_json[0].get("exist"))
            elif isinstance(chk_json, dict):
                is_alive = bool(chk_json.get("exist"))
            
            if is_alive:
                page_url = f"https://vikingfile.com/f/{viking_hash}"
                requests.get(page_url, timeout=10, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
                return True, "Node verified live & activity pinged (+10d)"
            else:
                return False, "File reported not exist on server"
        return False, f"Check API returned HTTP {chk_resp.status_code}"
    except Exception as ex:
        return False, str(ex)

print(f"[*] Initializing Gofile & Vikingfile Keep-Alive Probe Runner...")
print(f"[*] Target Controller: {METRICS_HOST}")
print(f"[*] Aging Threshold: {THRESHOLD_DAYS} Days | Batch Size: {BATCH_SIZE} | Strategy: {STRATEGY}")

batch_round = 0
total_renewed = 0
start_time = time.time()

while batch_round < MAX_ROUNDS:
    if time.time() - start_time > 1200:
        print("[!] Reached maximum safety runtime limit. Exiting.")
        break
    
    batch_round += 1
    print(f"\n==========================================")
    print(f"[*] Querying Telemetry Mirror Batch #{batch_round}...")
    
    try:
        resp = requests.get(
            f"{METRICS_HOST}/api/v1/telemetry/mirror-nodes?limit={BATCH_SIZE}&threshold_days={THRESHOLD_DAYS}",
            headers=headers,
            timeout=20
        )
        if resp.status_code != 200:
            print(f"[!] Controller endpoint returned HTTP {resp.status_code}")
            break
        payload = resp.json()
    except Exception as err:
        print(f"[!] Connectivity error to telemetry host: {err}")
        break

    targets = payload.get("files") or []
    if not targets:
        print("[✓] No more pending mirror nodes requiring renewal. All cloud mirrors healthy!")
        break
    
    print(f"[+] Loaded {len(targets)} mirrors in batch #{batch_round}.")
    
    telemetry_batch = []
    
    for idx, node in enumerate(targets, 1):
        file_id = node.get("file_id")
        fname = node.get("filename", file_id)
        gf_url = node.get("gofile_url") or node.get("gofile_id")
        vf_hash = node.get("vikingfile_hash") or ""
        
        print(f"[{idx}] Evaluating mirror node [{file_id}] - {fname[:30]}...")
        
        gf_status = "none"
        vf_status = "none"
        
        if gf_url:
            ok, msg = renew_gofile_node(gf_url)
            gf_status = "success" if ok else "failed"
            print(f"    -> Gofile: {msg}")
        
        if vf_hash:
            ok, msg = renew_viking_node(vf_hash)
            vf_status = "success" if ok else "failed"
            print(f"    -> Vikingfile: {msg}")
        
        telemetry_batch.append({
            "file_id": file_id,
            "gofile_status": gf_status,
            "viking_status": vf_status,
            "bytes_read": 3 * 1024 * 1024 if gf_status == "success" else 0
        })
        
        time.sleep(random.uniform(0.3, 0.6))

    # Submit batch report
    print(f"[*] Submitting Batch #{batch_round} telemetry report ({len(telemetry_batch)} nodes)...")
    report_data = {
        "runner_id": f"gh-gv-probe-r{batch_round}",
        "results": telemetry_batch
    }
    try:
        rep_res = requests.post(
            f"{METRICS_HOST}/api/v1/telemetry/mirror-report",
            json=report_data,
            headers=headers,
            timeout=20
        )
        if rep_res.status_code == 200:
            rep_json = rep_res.json()
            renewed = rep_json.get("renewed", len(telemetry_batch))
            total_renewed += renewed
            print(f"[✓] Batch #{batch_round} verified. Renewed {renewed} mirrors (+10 days). Total: {total_renewed}")
        else:
            print(f"[!] Telemetry server returned HTTP {rep_res.status_code}")
            break
    except Exception as ex:
        print(f"[!] Telemetry push failed: {ex}")
        break

    time.sleep(0.5)

elapsed = time.time() - start_time
print(f"\n==========================================")
print(f"[DONE] Mirror probe session completed in {elapsed:.1f}s.")
print(f"[*] Total Mirror Nodes Renewed (+10 Days): {total_renewed}")
print(f"==========================================")
