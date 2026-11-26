# AWS runbook: LSST + Roman chunking side-by-side

**What you'll do:** about 1 hour of clicking in the AWS web console. You'll end with one cloud machine (an "EC2 instance") that Claude Science can log into, plus one private storage bucket (an "S3 bucket"). After that I install the software and run everything.

**What it should cost:** roughly **$5–10 in total**, probably all covered by your $100 sign-up credit, and capped by the project spend limit you'll set in A3. The machine bills only while it is running, about $0.15–0.40 per hour.

**Where to stop if unsure:** anything not described here, leave at its default.

Console labels below match the AWS console as of late 2026. AWS renames things occasionally; if a label differs slightly, pick the obvious equivalent.

---

## Part A — Account and safety rails (about 15 min, once only)

Your account uses AWS's **new project-based experience**: the account menu at the top-right shows a project name (e.g. "Ready Set Build"), Free plan status, Projects, Team, Billing and Profile. That layout has three consequences for this runbook:

- **Region:** everything runs in your project's region, **US East (Ohio) us-east-2**, which AWS chose from your address.
- **No extra admin user needed:** there are no IAM users to create, so skip anything about Identity Center.
- **Roman data:** STScI's public bucket is in N. Virginia. Anonymous reads of it from Ohio will probably work (to be tested on the machine's first day). Either way, I'll copy the 18 Roman files (3.7 GB) into your Ohio bucket so the main comparison is same-region. Nothing for you to do about that.

**A1. Account.** Done.

**A2. Turn on MFA.**
1. Click your **name / project** menu at the top-right → **Profile**. It opens in a new tab.
2. Find **Multi-factor authentication (MFA)** (it may be under a "Security" heading) → **Register device** (or **Add MFA device**).
3. Choose **Authenticator app**, scan the QR code with an authenticator app on your phone, and enter the codes it shows.
4. If a device is already listed, you're done.

**A3. Make a project for this work, on the Paid plan with a spend limit.**
AWS already created one project for you at sign-up (e.g. "Ready Set Build"). Each project is its own AWS account, and its **AWS Console** button opens the console for that account. A dedicated project keeps this benchmark's spending cap separate and makes cleanup a single "delete project".

The Free plan may refuse the 8-CPU machine we need. On the Paid plan your **$100 credit is still used first**, and the **spend limit** makes AWS halt the project rather than bill past a cap you set.

1. Account menu → **Projects** → **Create project** → name it `chunkbench`.
2. If the creation flow offers a plan and a spend limit, choose the **Paid plan** and **$25 / month** (AWS's minimum is $20, or its own estimate of your spend if higher). Add a payment method if asked; credits are still applied first.
3. If it doesn't offer those, open the new project by clicking its name. Look for **Upgrade to Paid plan** / **Spend limit** there, or under account menu → **Billing**, and set $25.
4. **From now on, do everything in Parts B and C through the `chunkbench` project's _AWS Console_ button.** Resources belong to whichever project's console you create them in.

(Using the existing project instead is fine too: upgrade it and set the spend limit the same way.)

Note: AWS can start blocking new resources a few days before a project is projected to hit its limit. We'll be far below $25, but if a launch is unexpectedly refused, check here first.

**A4. Check the region.** The region menu at the top-right of the console (inside EC2, S3 etc.) should read **US East (Ohio) us-east-2**. Leave it there. If it shows another region and won't let you change it, tell me what it says.

---

## Part B — Storage and permissions (about 15 min)

**B1. Create the private bucket.**
1. Search **S3** → **Create bucket**.
2. **Bucket name:** must be unique across all of AWS. Use something like `jeg-chunkbench-` followed by today's date, e.g. `jeg-chunkbench-20261125`. Write it down.
3. **Region:** **US East (Ohio) us-east-2** (it may already be fixed to this).
4. Leave **Block all public access** ON (the default). The bucket will hold a private copy of DP1 data.
5. Leave everything else at its default → **Create bucket**.

**B2. Create a role that lets the machine use only that bucket.**
1. Search **IAM** → **Roles** → **Create role**.
2. **Trusted entity type:** AWS service. **Use case:** EC2 → **Next**.
3. On "Add permissions", don't tick anything → **Next**.
4. **Role name:** `chunkbench-ec2` → **Create role**.
5. Open the new role → **Add permissions → Create inline policy** → choose the **JSON** tab.
6. Paste the contents of `s3_policy_template.json`, replacing both copies of `YOUR-BUCKET-NAME` with your bucket name → **Next**.
7. **Policy name:** `chunkbench-bucket-only` → **Create policy**.

This role can read and write your one bucket and nothing else.

**If the console refuses to create the role or the inline policy** (a project restriction), stop and tell me the exact message; there's a workaround.

---

## Part C — The machine (about 15 min)

**C1. Make an SSH key.**
1. Search **EC2** (make sure the region still says **Ohio**).
2. Left menu **Network & Security → Key Pairs** → **Create key pair**. Name `chunkbench`, type **RSA**, format **.pem** → **Create**. A file `chunkbench.pem` downloads.
3. Move it somewhere safe and lock it down. In Terminal on your Mac:

   ```bash
   mkdir -p ~/.ssh && mv ~/Downloads/chunkbench.pem ~/.ssh/ && chmod 400 ~/.ssh/chunkbench.pem
   ```

**C2. Launch the instance.** EC2 → **Instances → Launch instances**:

| field | value |
|---|---|
| **Name** | `chunkbench` |
| **Application and OS Images** | **Ubuntu** → **Ubuntu Server 24.04 LTS (HVM), SSD Volume Type**, architecture **64-bit (x86)** |
| **Instance type** | **m7i.2xlarge** (8 vCPU, 32 GiB). If it isn't offered, use **m6i.2xlarge** |
| **Key pair** | `chunkbench` |
| **Network settings** → **Edit** | keep "Create security group"; the rule is **SSH, port 22**, and set **Source type** to **My IP** |
| **Configure storage** | change 8 GiB to **120** GiB, type **gp3** |
| **Advanced details → IAM instance profile** | `chunkbench-ec2` |
| **Advanced details → Purchasing option** | tick **Spot instances**. Then **Customize**: **Request type: Persistent**; **Interruption behavior: Stop** |
| **Advanced details → User data** (at the very bottom) | paste the whole of `aws_bootstrap.sh` |

Then **Launch instance**.

About Spot: it is spare AWS capacity at a large discount, but AWS can reclaim it at short notice. "Persistent + Stop" means that if that happens, the machine is stopped rather than deleted, so its disk and installed software survive. Results are saved as we go.

If launch fails with a message about **Spot**, a **vCPU limit**, or a **policy / not authorized**, which is common on brand-new accounts and projects, untick **Spot instances** and try again (normal on-demand pricing, about $0.40/hour, still cheap). If it still fails, try **m7i.xlarge** (4 vCPU, 16 GiB). If that fails too, send me the exact message.

**C3. Give it a fixed address.** By default its address changes every time it stops.
1. EC2 → **Network & Security → Elastic IPs** → **Allocate Elastic IP address** → **Allocate**.
2. Select it → **Actions → Associate Elastic IP address** → Instance: `chunkbench` → **Associate**.
3. Note the IP (e.g. `3.91.x.x`). It costs about half a cent per hour, the same as any public address.

**C4. Check you can log in.** Wait about 3 minutes for the boot script, then in Terminal:

```bash
ssh -i ~/.ssh/chunkbench.pem ubuntu@<ELASTIC-IP>
cat ~/BOOTSTRAP_DONE      # should print "bootstrap done ..."; if not, wait a minute and retry
```

If `ssh` times out, your home IP may have changed. Go to EC2 → the instance → **Security** tab → the security group → **Edit inbound rules**, and set Source to **My IP** again.

**C5. Put your RSP token on the machine** (needed to read DP1 through the Butler). While logged in:

```bash
read -rs T && printf '%s' "$T" > ~/.rsp_token && chmod 600 ~/.rsp_token && unset T
```

Paste the token at the blank prompt (nothing will show) and press Enter. Doing it this way keeps the token out of the shell history. Then type `exit`.

---

## Part D — Connect it to Claude Science (about 5 min)

1. Optional but handy: add an entry to `~/.ssh/config` on your Mac:

   ```
   Host chunkbench
       HostName <ELASTIC-IP>
       User ubuntu
       IdentityFile ~/.ssh/chunkbench.pem
   ```

2. In Claude Science: **Customize** (left sidebar) → **Compute** → add an **SSH** host. Use host `chunkbench` (or the Elastic IP), user `ubuntu`, and the key file `~/.ssh/chunkbench.pem`.
3. Then tell me in chat: "the host is added", plus your **bucket name**.

From here I take over: install the LSST Science Pipelines (with the Butler) and the Roman stack, copy the 200 DP1 visits into a private Butler in your bucket, and run the benchmark.

**A note on DP1 data rights:** the copy in your bucket is a private copy for your own analysis. Keep the bucket private (the default) and delete it at the end. If in doubt, check Rubin's data-rights policy.

---

## Day-to-day

- **Pause billing for the machine:** EC2 → Instances → select → **Instance state → Stop**. The disk (about $0.32/day) and Elastic IP (about $0.12/day) still bill while stopped. **Start** resumes it with the same IP.
- **Check spend:** Billing and Cost Management → **Bills** (or **Cost Explorer**). Charges appear with a delay of hours.

## Teardown (when we're done)

1. EC2 → Instances → select → **Instance state → Terminate**. For a Spot instance, also go to **Spot Requests**, select the request → **Actions → Cancel request**, otherwise a persistent request can relaunch it.
2. EC2 → **Elastic Block Store → Volumes**: delete any leftover `available` volume. **Snapshots**: delete any.
3. EC2 → **Elastic IPs**: select → **Actions → Release Elastic IP address**.
4. S3 → your bucket → **Empty** → then **Delete**.
5. Optional: IAM → Roles → delete `chunkbench-ec2`. EC2 → Key Pairs → delete `chunkbench`. Security Groups → delete the one the launch created.
6. Next day: Billing → **Bills** to confirm nothing is still running.
7. Delete the `chunkbench` project (account menu → **Projects**). This also removes anything left inside it. Or keep it, since an empty project costs nothing.

## Expected costs (us-east-2 Ohio, approximate; check the pricing pages)

| item | rate | likely total |
|---|---|---|
| m7i.2xlarge Spot (on-demand is about $0.40/h) | about $0.15–0.25 / hour | $2–4 (10–15 h of actual use) |
| 120 GB gp3 disk | about $0.08 / GB-month, about $0.32/day | about $1–2 |
| Elastic IP / public IPv4 | $0.005 / hour | about $0.50 |
| S3 storage, about 25 GB for a week | about $0.023 / GB-month | about $0.15 |
| S3 requests (about 100k GETs) | $0.0004 per 1k | about $0.05 |
| Data transfer within us-east-2 (your bucket to EC2) | free | $0 |
| Copy of 18 Roman files from STScI (3.7 GB, anonymous reads of a public bucket) | billed to the bucket owner (STScI / AWS Open Data), not you | $0 |
| Download from Rubin's Google Cloud bucket into AWS (about 21 GB) | billed to Rubin, not you | $0 |
| **Total** | | **about $5–10** |
