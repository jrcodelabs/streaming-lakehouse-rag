# Build Journal — Streaming Lakehouse + RAG Analytics Assistant

A chronological record of the actual build process: what was set up, what broke, what was tried, and what eventually fixed it. Unlike `SETUP.md` (the clean "how to rebuild this" guide), this document preserves the real troubleshooting path — including dead ends — as a record of the engineering process.

**Note on accuracy:** this was reconstructed from conversation history, including several steps inferred from pasted terminal output/screenshots rather than directly stated commands. Items marked *(likely)* are best-effort reconstructions — review and correct anything that doesn't match what you actually ran.

---

## Phase 1 — Planning & environment groundwork (~Aug 14)

- Defined the 8-week capstone plan (Kafka → Databricks → dbt → RAG → Streamlit) and confirmed scope: hands-on, free, ~10–15 hrs/week, one integrated project using a general public dataset.
- Chose a GitHub handle/profile name and created the public repo `streaming-lakehouse-rag` under account `jrcodelabs`.
- Walked through repo creation options (visibility, README, `.gitignore`, license) — chose **MIT License** after asking what it meant.
- Clarified conceptually what Docker is and how it related to the project, after initially asking "what is docker ... I experience only on linux" — then clarified the actual dev machine is **Windows**, not Linux, which changed the setup path.

### Installing Docker Desktop + WSL2
- Asked which specific link to use from Docker's Windows install docs; installed **Docker Desktop for Windows**.
- **Error hit:** `Windows Subsystem for Linux (WSL) is not installed`
  **Fix:** ran `wsl --install` in an **administrator PowerShell**, then rebooted Windows. This installed WSL2 + Ubuntu.
- Tested Docker with `docker run -it ubuntu bash` — confirmed it worked (prompt showed `root@<container-id>:/#`).
- Asked how to get into WSL2 directly (vs. via a Docker container) — opened the **Ubuntu** app from the Start menu, landing at a prompt like `jubyk@DESKTOP-GL7Q6E7:~$`.
- Clarified that this WSL2 home directory is a real location on the Windows machine (a separate Linux filesystem, not literally inside `C:\`), and confirmed it was fine to create all current/future project repos there.
- Confirmed the plan step "sign up for Databricks" was independent of the `docker-compose.yml` step — they could be done in either order.

### Python
- Asked which specific installer link to use from `python.org` (version 3.14.7 release page). *(likely a Windows-side consideration at the time — WSL2's Ubuntu later turned out to already have Python 3.14 available via `apt`, confirmed much later by the `externally-managed-environment` pip error referencing `python3.14`.)*

### NYC TLC taxi data
- Asked what to download from the NYC TLC Trip Record Data page and where, since the project lives under the Ubuntu/WSL2 shell.
- Learned `curl -O` (download a file, keep its original filename) and that `curl` stands for **Client URL**.
- Downloaded 2–3 months of Yellow Taxi Parquet files into `data/raw/`.

### Recap/conceptual check-ins during this phase
- "Why did we install Docker?" — recap of Docker's role (running Kafka, later Databricks-adjacent tooling, all in reproducible containers).
- "I think we installed WSL2/Ubuntu also using Docker, correct?" — clarified: **no**, WSL2 was installed separately via `wsl --install`; Docker Desktop then *integrates with* WSL2, it didn't install it.

---

## Phase 2 — Git/GitHub setup

- Discussed whether `.gitignore` changes should be made directly on GitHub's website or by cloning locally first — settled on: clone/create locally, edit, then push.
- Realized no clone had actually been done yet — the repo was **manually created** at `~/projects/streaming-lakehouse-rag` instead.
- Asked where to create `data/raw/` — created under the project root.
- Removed an extra `source` directory that had been created by mistake.
- Asked about sequencing: sync git first, or add raw files + edit `.gitignore` first? (Addressed before any Kafka work had started, since Kafka was explicitly a "Week 1" task not yet begun.)

### SSH authentication
- **Error hit:** `git push` failed with **"Password authentication is not supported for Git operations."** (screenshot)
- Checked `git config --list` to confirm the configured email.
- Asked whether to use the real email or a GitHub **noreply** email for `ssh-keygen` — discussed the tradeoff (privacy vs. simplicity).
- **Fix:** generated an SSH key (`ssh-keygen -t ed25519`), started `ssh-agent`, added the key, registered the public key on GitHub, and updated the remote:
  ```bash
  git remote set-url origin git@github.com:jrcodelabs/streaming-lakehouse-rag.git
  ```
- Push succeeded. Saw GitHub's standard message *"Hi jrcodelabs! You've successfully authenticated, but GitHub does not provide shell access."* — confirmed this is expected/normal (SSH auth test message, not an error).

### Folder structure confusion
- At one point cloned a repo accidentally under `~/jubyk` instead of the intended `~/jubyk/projects`-style structure — removed the stray clone.
- *(likely)* `git status` once reported **"not currently on any branch"** after a `git clone --branch <tag>` — this is **detached HEAD state**, which happens when checking out a specific tag/commit rather than a branch; explained how to recognize and recover from it (`git checkout main` or `git checkout -b <new-branch>`).

---

## Phase 3 — LLM API key decision

- Asked whether the existing **Claude.ai subscription** could be used as the LLM key for this project.
  **Clarified:** Claude.ai's consumer subscription and the **Anthropic API** are billed separately — a chat subscription does not include API credits.
- Compared **Groq** ("generous free tier, no card required") vs. **Google AI Studio/Gemini** (free tier, tighter caps on Pro models) — chose Groq.
- *(Covered again in more detail later — see Phase 7.)*

---

## Phase 4 — Kafka conceptual deep-dive (parallel to hands-on work)

A cluster of conceptual questions, mostly answered directly rather than via hands-on steps:
- Refresher request on the Confluent Fundamentals course content.
- What Kubernetes is, and whether it relates to Kafka (orchestration, contrasted with Control-M-style job orchestration).
- Producer API vs. Consumer API vs. Kafka Streams (library) — and the general distinction between an "API" and a "library."
- Whether Kafka Streams can be used from Python (answer: not natively — it's a JVM library; Python alternatives like Faust/Quix Streams/Bytewax/PyFlink exist).
- Kafka Streams vs. Apache Flink vs. Kafka Connect — three different tools solving different problems.
- Why Kafka Connect exists at all, instead of hand-writing integrations against the Producer/Consumer APIs.

---

## Phase 5 — Confluent Fundamentals hands-on lab (separate clone)

- Reconnecting to the WSL2 terminal after it had closed (just reopening the Ubuntu app).
- **Error hit (first occurrence):** `The command 'docker' could not be found in this WSL 2 distro.`
  **Fix:** opened Docker Desktop and verified **WSL Integration** was enabled for the Ubuntu distro (Settings → Resources → WSL Integration → Apply & Restart). *(This same error recurred later — see below — most likely because Docker Desktop wasn't running in the background at the time, since the integration setting itself doesn't "un-set" on its own.)*
- Converted a multi-line `git clone` command (with trailing `\` line-continuations) into a single line, and confirmed install location:
  ```bash
  git clone --branch 7.8.1-v1.0.0 https://github.com/confluentinc/training-fundamentals-src.git ~/projects/confluent-fundamentals
  ```
- Covered broker vs. controller terminology from the lab material, and cleared up confusion between "node," "broker," and "server" (all roughly synonymous in this context — a node running the Kafka process).
- Ran the project's `update-hosts.sh`, which edits `/etc/hosts` to resolve hostnames like `kafka`, `zookeeper`, `postgres`, etc. Asked whether the Postgres entry meant DB access was available — **clarified: no**, it's only a hostname-to-IP mapping, not a grant of access/credentials.
- Read a lab PDF and asked whether `start.sh` was responsible for the "write" (producer) side of the pipeline, since only consumer-related commands were visible in the conclusion — confirmed yes, `start.sh` runs the producer.

### The `start.sh` "Kafka not yet ready" saga
- **Symptom:** running `start.sh` printed `Kafka not yet ready...` in an infinite loop — observed over **45+ minutes** with no progress.
- Investigated via `docker ps -a` and `docker logs kafka` — found:
  - A non-fatal `InvalidReplicationFactorException` for `_confluent-telemetry-metrics` (confirmed, via searching, to be known harmless noise).
  - Eventually, a **fatal** error: `Failed to start license store with topic _confluent-command`, also an `InvalidReplicationFactorException` — this one real, since the topic needs replication factor 3 but only one broker was running.
- Asked practical side questions while debugging: how to quit `nano` without saving changes, whether the machine going to sleep would interrupt a long-running process, and whether `nohup <cmd> &` would help keep it running in the background.
- **First fix attempt:** added `KAFKA_DEFAULT_REPLICATION_FACTOR: 1` to the broker's environment in `docker-compose.yml`.
  **Result: did NOT work** — the same fatal `_confluent-command` error recurred on the next run. (Root cause: the internal `CreateTopics` request hardcodes replication factor 3 directly, bypassing the broker-wide default setting.)
- **Confirmed working fix:** found via Confluent's own official `cp-all-in-one` reference `docker-compose.yml` on GitHub — added instead:
  ```yaml
  KAFKA_CONFLUENT_LICENSE_TOPIC_REPLICATION_FACTOR: 1
  ```
  Verified fixed via `docker logs kafka` (fatal error gone, only harmless telemetry noise remained) and `docker exec kafka kafka-topics --bootstrap-server kafka:9092 --list` (broker healthy, `_confluent-command` topic now present).
- **Separate root cause, found afterward:** even with the broker fixed, `start.sh`'s own readiness-check loop (`while ! kafka-topics --bootstrap-server kafka:9092 --list ...`) was still failing — because it runs `kafka-topics` directly on the WSL2 **host**, where that binary isn't installed (it only exists *inside* the `kafka` container). The condition silently fails with "command not found" rather than erroring loudly, since it's inside a `while` condition (exempt from `set -e`).
  **Recommended fix** *(not confirmed as applied)*: prefix both `kafka-topics` calls inside `start.sh` with `docker exec kafka`.
- Along the way, the original `docker: command not found` error **recurred** (date context suggests a gap in sessions, likely because Docker Desktop simply wasn't running at the time) — same fix applied again (start Docker Desktop, confirm WSL integration still enabled).
- A side detour: tried running `update-hosts.sh` from **Windows Git Bash** instead of WSL2, and hit `Sudo is disabled on this machine` — recommended **staying in WSL2** entirely rather than switching environments mid-troubleshooting, since all prior debugging context lived there.
- Read `stop.sh` (`cat stop.sh`) — confirmed it does proper cleanup: `docker container rm -f producer`, `docker container rm -f kafka`, `docker compose down -v`.
- Asked why a filename showed a trailing `*` in a directory listing — this is `ls -F`'s convention for marking an **executable** file.

### Manual topic interaction in the lab
- **Error hit:** `docker exec kafka kafka-console-producer --broker-list kafka:9092 --topic sample-topic` accepted no keyboard input.
  **Fix:** added `-it` flags: `docker exec -it kafka kafka-console-producer ...` (needed to attach STDIN/TTY).
- Asked why consumed message order looked "scrambled" — explained Kafka only guarantees ordering **within a single partition**, not across a topic's multiple partitions. *(No confirmed follow-up on checking `sample-topic`'s actual partition count.)*
- Asked why **Ctrl+Z** wouldn't exit the console consumer — clarified Ctrl+Z **suspends** (SIGTSTP), it doesn't terminate; **Ctrl+C** (SIGINT) is the correct key; covered recovery via `jobs` / `fg` / `kill %1` for an already-suspended process.
- **Error hit:** a `kafka-metadata-shell` command returned `--controllers: command not found` — root cause was a multi-line command split across two lines **without** a trailing backslash, so bash ran the second line as a separate (invalid) command. Fixed by combining onto one line.

### Confluent accreditation exam
- Walked through all **30 exam questions**, one at a time (via screenshots), each with the correct answer and reasoning tied back to concepts already covered.
- ✅ Completed the exam (30/30) and earned the Confluent Apache Kafka Fundamentals accreditation.

---

## Phase 6 — Back to the capstone repo: scaffolding & Python environment (~Oct 3–5)

- Verified Docker/Git were present and working (`docker ps`, `docker --version`, `git --version`).
- Confirmed Python 3.14 was already available in WSL2 via `apt` (not something separately installed in this session).

### Virtual environment
- **Error hit:** `pip install python-dotenv` failed with:
  `error: externally-managed-environment` (PEP 668 protection in newer Debian/Ubuntu Python).
  **Fix:** created and activated a project-local virtual environment instead of installing system-wide:
  ```bash
  python3 -m venv .venv
  source .venv/bin/activate
  pip install python-dotenv
  ```
- Covered how to list installed/available packages: `pip list`, `pip freeze`, `pip show <pkg>`, and the apt equivalents (`apt list --installed`, `apt-cache search`).

### Git workflow habits
- Walked through the full commit/push mechanics, clarifying that `git commit` is **local-only** and `git push` is the separate step that sends it to GitHub.
- Clarified `git add .` (stages everything under the current directory, respecting `.gitignore`) vs. naming a file explicitly.
- Adopted the **branch → PR → merge** workflow deliberately, rather than committing straight to `main`:
  ```bash
  git checkout -b <branch-name>
  # work, commit
  git push -u origin <branch-name>   # first push of a new branch needs -u
  # open PR on GitHub, review diff, merge, delete branch
  git checkout main && git pull
  ```
- Covered branch cleanup safety: `git branch --merged main` to confirm a branch's work is fully in `main` before deleting; `git branch -d <branch>` (safe delete, refuses if unmerged) vs. `-D` (force); `git push origin --delete <branch>` for the remote copy; `git fetch --prune` to clear stale remote-tracking refs.
- Clarified that deleting a **merged** branch is safe — the commits live on in `main`'s history regardless; only deleting an **unmerged** branch risks losing work (recoverable short-term via `git reflog`, but not reliably).

### Repo scaffolding
- Created `/producer`, `/kafka` *(see note below)*, `/notebooks`, `/dbt`, `/rag`, `.github/workflows/` directories, each with a placeholder `README.md` so Git would track the otherwise-empty folders.
- Wrote the top-level project `README.md`, including the architecture diagram.
- Hit a **documentation-only** nested-code-fence rendering glitch when the architecture diagram block (already fenced with triple backticks inside the file) was shown wrapped in another fence in chat — clarified this only affected the chat display, not the actual file.
- Did this scaffolding work via the branch → PR → merge flow (branch `scaffold/repo-structure` *(likely name)*).

### The missing `kafka/` directory
- When re-checking the project folder (`ll`) ahead of the Kafka producer step, **`kafka/docker-compose.yml` was missing entirely** — it appears the Week 1 Redpanda setup was never actually committed (or was lost/never saved) despite being referenced throughout earlier conversation.
- **Fix:** recreated `kafka/docker-compose.yml` from scratch (Redpanda + Redpanda Console, same config as originally used) on the `feature/kafka-producer` branch, and confirmed via `docker ps` that both containers (`redpanda`, `redpanda-console`) came up healthy.

---

## Phase 7 — Kafka producer build

- Created the `taxi-trips` topic explicitly (3 partitions) via `rpk`:
  ```bash
  docker exec -it redpanda rpk topic create taxi-trips --partitions 3
  ```
- Installed remaining packages: `pandas`, `pyarrow`, `kafka-python`.
- Wrote `producer/produce_trips.py` — reads the taxi Parquet files with pandas, serializes each row to JSON, and sends it to the `taxi-trips` topic via `kafka-python`, throttled with `time.sleep()` to simulate real-time arrival.
- Asked about the purpose of the `if __name__ == "__main__":` guard — covered how `__name__` differs between running a file directly vs. importing it as a module.
- Ran the producer and **confirmed events landing** in the Redpanda Console UI (`http://localhost:8080`) — Week 1/2 milestone achieved.
- Covered `git push` vs. `git push -u origin <branch>` (the `-u` is only needed the first time a new branch is pushed).

### Groq API key (completed)
- Signed up free at [console.groq.com](https://console.groq.com), created an API key.
- Stored it in a gitignored `.env` file (`GROQ_API_KEY=...`), loaded via `python-dotenv`'s `load_dotenv()` — set up in advance of the Week 6/7 RAG work.

---

## Phase 8 — Confluent certificate placement

- Discussed where to surface the completed Confluent accreditation: primarily **LinkedIn** (Licenses & Certifications — searchable by recruiters), also the project **README** (as a verified badge/link, not an uploaded certificate file), and the **GitHub profile README**. Decided against committing the certificate file itself into the repo.

---

## Open items / not yet confirmed

- Whether `start.sh` was ever actually patched with the `docker exec kafka` prefix fix for its readiness-check loop.
- Whether `sample-topic`'s partition count was ever checked to fully explain the "scrambled order" observation.
- Exact completion date of the Confluent accreditation (for dating the README/LinkedIn entries).
