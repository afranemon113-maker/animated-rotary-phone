#!/usr/bin/env python3
"""
MHB Automation Engine - GitHub Actions Cluster Re-uploader
Streams media directly from Google GPMC / Cloudflare Edge and re-uploads
in PARALLEL (Multi-threaded) to Pixeldrain, Gofile, Vikingfile, and Cloudflare R2
with ZERO VPS bandwidth.
"""

import os
import sys
import time
import json
import re
import argparse
import requests
import boto3
import concurrent.futures
from botocore.config import Config

def log(msg):
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {msg}", flush=True)

def resolve_driveshare(file_id, resolver_url, resolver_key):
    log(f"[DriveShare] Resolving remote links for file ID: {file_id} ...")
    params = {
        "api_key": resolver_key,
        "url": file_id
    }
    r = requests.get(resolver_url, params=params, timeout=15)
    if r.status_code != 200:
        raise Exception(f"DriveShare Resolver returned HTTP {r.status_code}: {r.text}")
    
    data = r.json()
    filename = data.get("filename", f"file_{file_id}.mkv")
    file_size = int(data.get("file_size", 0))
    links = data.get("links", {})

    existing_r2_url = links.get("r2_public_url") or links.get("r2_download_url") or ""

    # Priority for downloading source: 1st GPMC -> 2nd R2 -> 3rd CFWorker -> 4th Drive
    source_url = None
    source_name = None

    if links.get("gpmc_link"):
        source_url = links["gpmc_link"]
        source_name = "gpmc"
    elif existing_r2_url:
        source_url = existing_r2_url
        source_name = "r2"
    elif links.get("cfworker_link"):
        source_url = links["cfworker_link"]
        source_name = "cfworker"
    elif links.get("original_drive_link"):
        source_url = links["original_drive_link"]
        source_name = "drive"
    else:
        raise Exception(f"No valid streaming source found in DriveShare response for {file_id}")

    log(f"[DriveShare] Selected source: {source_name.upper()} | Filename: {filename} | Size: {file_size / (1024*1024):.2f} MB")
    return {
        "filename": filename,
        "file_size": file_size,
        "source_url": source_url,
        "source_name": source_name,
        "existing_r2_url": existing_r2_url
    }

def download_source(source_url, local_path, expected_size=0):
    log(f"[Downloader] Initiating high-speed stream from: {source_url[:70]}...")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 MHB-Cloud-Runner/2.0"
    }
    r = requests.get(source_url, headers=headers, stream=True, timeout=30)
    if r.status_code not in (200, 206):
        raise Exception(f"Source download failed with HTTP {r.status_code}")

    total_downloaded = 0
    start_time = time.time()
    last_print = start_time

    with open(local_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 1024 * 2): # 2MB chunks
            if chunk:
                f.write(chunk)
                total_downloaded += len(chunk)
                now = time.time()
                if now - last_print >= 5: # log every 5s
                    elapsed = now - start_time
                    speed_mbps = (total_downloaded * 8 / (1024 * 1024)) / (elapsed if elapsed > 0 else 1)
                    pct = (total_downloaded / expected_size * 100) if expected_size > 0 else 0
                    log(f"[Downloader] Progress: {total_downloaded / (1024*1024):.1f} MB ({pct:.1f}%) | Speed: {speed_mbps:.1f} Mbps")
                    last_print = now

    elapsed = time.time() - start_time
    avg_speed = (total_downloaded * 8 / (1024 * 1024)) / (elapsed if elapsed > 0 else 1)
    log(f"[Downloader] Download completed! Total: {total_downloaded / (1024*1024):.1f} MB in {elapsed:.1f}s (Avg: {avg_speed:.1f} Mbps)")
    return total_downloaded

def upload_cloudflare_r2(local_path, filename, account_id, access_key, secret_key, bucket_name, custom_domain):
    log(f"[Cloudflare R2] Connecting to R2 bucket: '{bucket_name}' (Account: {account_id[:8]}...)...")
    clean_filename = re.sub(r'[^a-zA-Z0-9._-]', '_', filename)
    safe_key = f"streams/{int(time.time())}/{clean_filename}"
    
    s3 = boto3.client(
        's3',
        endpoint_url=f'https://{account_id}.r2.cloudflarestorage.com',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version='s3v4'),
        region_name='auto'
    )
    
    file_size = os.path.getsize(local_path)
    log(f"[Cloudflare R2] Uploading {clean_filename} ({file_size / (1024*1024):.1f} MB) -> {safe_key} ...")
    
    extra_args = {
        'ContentType': 'video/mp4',
        'ContentDisposition': f'attachment; filename="{clean_filename}"'
    }
    
    s3.upload_file(local_path, bucket_name, safe_key, ExtraArgs=extra_args)
    
    domain = (custom_domain or '').rstrip('/')
    if not domain:
        domain = f"https://{bucket_name}.r2.dev"
    elif not domain.startswith('http'):
        domain = f"https://{domain}"
        
    public_url = f"{domain}/{safe_key}"
    log(f"[Cloudflare R2] Upload SUCCESS! Public URL: {public_url}")
    return public_url, safe_key

def upload_pixeldrain(local_path, filename, api_key):
    log("[Pixeldrain] Uploading to Pixeldrain Cloud...")
    url = f"https://pixeldrain.com/api/file/{requests.utils.quote(filename)}"
    auth = ("", api_key)
    file_size = os.path.getsize(local_path)
    
    with open(local_path, "rb") as f:
        headers = {
            "Content-Length": str(file_size),
            "User-Agent": "MHB-Automation-Engine-Go/1.0"
        }
        r = requests.put(url, auth=auth, data=f, headers=headers, timeout=900)
    
    if r.status_code not in (200, 201):
        raise Exception(f"Pixeldrain upload failed: HTTP {r.status_code} - {r.text}")
    
    resp_json = r.json()
    pd_id = resp_json.get("id")
    if not pd_id:
        raise Exception(f"Pixeldrain response missing file ID: {r.text}")
    
    view_url = f"https://pixeldrain.com/u/{pd_id}"
    log(f"[Pixeldrain] Upload SUCCESS! ID: {pd_id} | URL: {view_url}")
    return pd_id, view_url

def upload_gofile(local_path, filename):
    log("[Gofile] Fetching best Gofile upload server...")
    srv_resp = requests.get("https://api.gofile.io/servers", timeout=10)
    server = "store1"
    if srv_resp.status_code == 200:
        data = srv_resp.json()
        servers = data.get("data", {}).get("servers", [])
        if servers and len(servers) > 0:
            server = servers[0].get("name", "store1")
    
    upload_url = f"https://{server}.gofile.io/contents/uploadfile"
    log(f"[Gofile] Uploading to {upload_url}...")
    
    with open(local_path, "rb") as f:
        files = {"file": (filename, f)}
        r = requests.post(upload_url, files=files, timeout=900)
    
    if r.status_code not in (200, 201):
        raise Exception(f"Gofile upload failed: HTTP {r.status_code} - {r.text}")
    
    res = r.json()
    if res.get("status") not in ("ok", "success"):
        raise Exception(f"Gofile error: {res}")
    
    data = res.get("data", {})
    page_url = data.get("downloadPage") or data.get("directLink")
    file_id = data.get("fileId", "")
    log(f"[Gofile] Upload SUCCESS! File ID: {file_id} | Page: {page_url}")
    return file_id, page_url

def upload_vikingfile(local_path, filename, user_hash):
    log("[Vikingfile] Initiating Vikingfile S3 Multipart upload...")
    file_size = os.path.getsize(local_path)
    
    init_resp = requests.post(
        "https://vikingfile.com/api/get-upload-url",
        data={"size": file_size},
        timeout=15
    )
    
    if init_resp.status_code == 200:
        init_data = init_resp.json()
        urls = init_data.get("urls", [])
        part_size = int(init_data.get("partSize", 0))
        upload_id = init_data.get("uploadId")
        key = init_data.get("key")
        
        if urls and part_size > 0:
            log(f"[Vikingfile] S3 Multipart plan: {len(urls)} parts (~{part_size / (1024*1024):.1f} MB each)")
            completed_parts = []
            with open(local_path, "rb") as f:
                for idx, part_url in enumerate(urls):
                    part_num = idx + 1
                    chunk = f.read(part_size)
                    put_resp = requests.put(part_url, data=chunk, timeout=300)
                    if put_resp.status_code not in (200, 201):
                        raise Exception(f"Vikingfile part {part_num} failed: HTTP {put_resp.status_code}")
                    etag = put_resp.headers.get("ETag", "").strip('"')
                    completed_parts.append({"PartNumber": part_num, "ETag": etag})
                    if part_num % 10 == 0 or part_num == len(urls):
                        log(f"[Vikingfile] Part {part_num}/{len(urls)} uploaded.")
            
            # Complete multipart upload
            complete_data = {
                "key": key,
                "uploadId": upload_id,
                "name": filename,
                "user": user_hash
            }
            for i, cp in enumerate(completed_parts):
                complete_data[f"parts[{i}][PartNumber]"] = str(cp["PartNumber"])
                complete_data[f"parts[{i}][ETag]"] = cp["ETag"]
            
            comp_resp = requests.post("https://vikingfile.com/api/complete-upload", data=complete_data, timeout=30)
            if comp_resp.status_code == 200:
                comp_json = comp_resp.json()
                vf_url = comp_json.get("url")
                if vf_url:
                    log(f"[Vikingfile] Upload SUCCESS! URL: {vf_url}")
                    return vf_url
    
    # Fallback to direct form upload
    log("[Vikingfile] S3 Multipart unavailable, using direct upload server...")
    with open(local_path, "rb") as f:
        files = {"file": (filename, f)}
        data = {"user_hash": user_hash}
        r = requests.post("https://upload.vikingfile.com/api/upload", files=files, data=data, timeout=900)
        if r.status_code == 200:
            vf_json = r.json()
            vf_url = vf_json.get("url")
            if vf_url:
                log(f"[Vikingfile] Direct Upload SUCCESS! URL: {vf_url}")
                return vf_url

    raise Exception("Vikingfile upload failed on both S3 multipart and direct API")

def send_callback(callback_url, callback_token, payload):
    log(f"[Callback] Reporting results back to MHB Engine: {callback_url} ...")
    headers = {
        "Content-Type": "application/json",
        "X-API-Key": callback_token
    }
    r = requests.post(callback_url, json=payload, headers=headers, timeout=20)
    log(f"[Callback] Engine response: HTTP {r.status_code} - {r.text}")
    return r.status_code == 200

def main():
    parser = argparse.ArgumentParser(description="MHB GitHub Cluster Re-uploader (Multi-Threaded Parallel)")
    parser.add_argument("--file-id", required=True, help="Unique file ID (e.g. 65QGP9FKB5)")
    parser.add_argument("--resolver-url", default="https://new.driveshare.top/api/v1/resolve")
    parser.add_argument("--resolver-key", default="api001_409ab1cf0b0b3894ce46b15c96b20e8c")
    parser.add_argument("--callback-url", default="https://one.neonxcloud.top/api/v1/reupload/callback")
    parser.add_argument("--callback-token", default="mhb_live_f1286e646e6f56c023bfd4b5d22b018384a07ba8")
    parser.add_argument("--pixeldrain-key", default="2cdd0854-c25e-47bb-8bb4-876a77d50a41")
    parser.add_argument("--vikingfile-hash", default="gZvSn4nejj")
    parser.add_argument("--target-pixeldrain", default="true")
    parser.add_argument("--target-gofile", default="true")
    parser.add_argument("--target-vikingfile", default="true")
    parser.add_argument("--target-r2", default="true")
    parser.add_argument("--r2-account-id", default="")
    parser.add_argument("--r2-access-key", default="")
    parser.add_argument("--r2-secret-key", default="")
    parser.add_argument("--r2-bucket", default="")
    parser.add_argument("--r2-domain", default="")
    
    args = parser.parse_args()
    file_id = args.file_id.strip()

    log("===========================================================")
    log(f"Starting GitHub Parallel Cluster Re-upload for file: {file_id}")
    log("===========================================================")

    # Step 1: Resolve source link via DriveShare
    res = resolve_driveshare(file_id, args.resolver_url, args.resolver_key)
    filename = res["filename"]
    expected_size = res["file_size"]
    source_url = res["source_url"]
    source_name = res["source_name"]
    existing_r2_url = res.get("existing_r2_url", "")

    # Step 2: Download high-speed to local temporary file
    temp_file = f"/tmp/reupload_{file_id}_{int(time.time())}.mkv"
    if os.name == "nt": # Windows fallback for local debugging
        temp_file = os.path.join(os.environ.get("TEMP", "."), f"reupload_{file_id}.mkv")

    try:
        actual_size = download_source(source_url, temp_file, expected_size)

        # Step 3: PARALLEL MULTI-THREADED RE-UPLOAD TO ALL ACTIVE TARGETS
        log("🚀 [Multi-Thread] Launching concurrent parallel upload workers...")
        upload_start = time.time()

        pd_id, pd_url = "", ""
        gf_id, gf_url = "", ""
        vf_url = ""
        r2_url = existing_r2_url
        r2_key = ""

        def worker_r2():
            nonlocal r2_url, r2_key
            if r2_url:
                log(f"[R2 Worker] Existing R2 link found: {r2_url} (Keeping existing link)")
                return
            if args.target_r2.lower() == "true" and args.r2_account_id and args.r2_access_key and args.r2_secret_key:
                log(f"[R2 Worker] Uploading to Cloudflare R2 bucket: '{args.r2_bucket}' ...")
                try:
                    r2_url, r2_key = upload_cloudflare_r2(
                        temp_file, filename,
                        args.r2_account_id,
                        args.r2_access_key,
                        args.r2_secret_key,
                        args.r2_bucket,
                        args.r2_domain
                    )
                except Exception as e:
                    log(f"[R2 Worker] ERROR: {e}")

        def worker_pixeldrain():
            nonlocal pd_id, pd_url
            if args.target_pixeldrain.lower() == "true":
                log("[Pixeldrain Worker] Uploading to Pixeldrain Cloud...")
                try:
                    pd_id, pd_url = upload_pixeldrain(temp_file, filename, args.pixeldrain_key)
                except Exception as e:
                    log(f"[Pixeldrain Worker] ERROR: {e}")

        def worker_gofile():
            nonlocal gf_id, gf_url
            if args.target_gofile.lower() == "true":
                log("[Gofile Worker] Uploading to Gofile Cloud...")
                try:
                    gf_id, gf_url = upload_gofile(temp_file, filename)
                except Exception as e:
                    log(f"[Gofile Worker] ERROR: {e}")

        def worker_vikingfile():
            nonlocal vf_url
            if args.target_vikingfile.lower() == "true":
                log("[Vikingfile Worker] Uploading to Vikingfile Cloud...")
                try:
                    vf_url = upload_vikingfile(temp_file, filename, args.vikingfile_hash)
                except Exception as e:
                    log(f"[Vikingfile Worker] ERROR: {e}")

        # Execute all 4 workers in parallel threads
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            tasks = {
                executor.submit(worker_r2): "Cloudflare R2",
                executor.submit(worker_pixeldrain): "Pixeldrain",
                executor.submit(worker_gofile): "Gofile",
                executor.submit(worker_vikingfile): "Vikingfile"
            }
            for future in concurrent.futures.as_completed(tasks):
                task_name = tasks[future]
                try:
                    future.result()
                    log(f"✅ [Multi-Thread] {task_name} finished successfully.")
                except Exception as exc:
                    log(f"❌ [Multi-Thread] {task_name} generated an unhandled exception: {exc}")

        total_upload_elapsed = time.time() - upload_start
        log(f"⚡ [Multi-Thread] All destination uploads finished in {total_upload_elapsed:.1f}s!")

        # Step 4: Report callback
        callback_payload = {
            "file_id": file_id,
            "filename": filename,
            "file_size": actual_size,
            "pixeldrain_id": pd_id,
            "pixeldrain_url": pd_url,
            "gofile_id": gf_id,
            "gofile_url": gf_url,
            "vikingfile_url": vf_url,
            "r2_url": r2_url,
            "r2_key": r2_key,
            "source_used": source_name,
            "status": "ready"
        }
        send_callback(args.callback_url, args.callback_token, callback_payload)
        log("✅ Cluster Re-upload completed successfully!")

    finally:
        # Cleanup
        if os.path.exists(temp_file):
            try:
                os.remove(temp_file)
                log("[Cleanup] Removed temporary download file.")
            except Exception:
                pass

if __name__ == "__main__":
    main()
