TODO
---

```bash
# Singularity container preparation
cd ~/diploma
singularity pull blender-docker.sif docker://linuxserver/blender:latest

# Generic way to run a single worker on the NSC cluster
singularity exec --bind /ceph/grid/home/sg7710/diploma/distributed-rendering-diploma/blender-projects --env RUST_LOG="debug" /ceph/grid/home/sg7710/diploma/blender-docker.sif /ceph/grid/home/sg7710/diploma/distributed-rendering-diploma/target/release/worker --masterServerHost nsc-login1 --masterServerPort 9901 --baseDirectory /ceph/grid/home/sg7710/diploma/distributed-rendering-diploma/ --blenderBinary /usr/bin/blender
```

---

# 1. Running performance tests on IJS' NSC

## 1.1 `01 Simple bobbing animation, 600 frames on 10 workers`


### *Strategy: naive, fine*
```bash
# Master (in a `screen` on the login1 instance)
cd ~/diploma/distributed-rendering-diploma
screen -t cm_simple-600f20w-fine -S cm_simple-600f20w-fine -L -Logfile cm_simple-600f20w-fine.log
RUST_LOG="debug" ./target/release/master --host 0.0.0.0 --port 9901 run-job --resultsDirectory ./blender-projects/01_simple-animation/results ./blender-projects/01_simple-animation/01-simple-animation_measuring_600f-20w_naive-fine.toml

# 20 workers (spawned with `srun`)
cd ~/diploma/distributed-rendering-diploma
screen -t cw_simple-600f20w-fine -S cw_simple-600f20w-fine -L -Logfile cw_simple-600f20w-fine.log
srun --job-name=cw_simple-600f20w-fine --ntasks=20 --output=cm_simple-600f20w-fine.workers.log --time=300 --mem=3G --cpus-per-task=4 --threads-per-core=1 -- singularity exec --bind /ceph/grid/home/sg7710/diploma/distributed-rendering-diploma/blender-projects --env RUST_LOG="debug" /ceph/grid/home/sg7710/diploma/blender-docker.sif /ceph/grid/home/sg7710/diploma/distributed-rendering-diploma/target/release/worker --masterServerHost nsc-login1 --masterServerPort 9901 --baseDirectory /ceph/grid/home/sg7710/diploma/distributed-rendering-diploma/ --blenderBinary /usr/bin/blender
```


### *Strategy: naive, coarse (target queue size = 5)*
```bash
# Master (in a `screen` on the login1 instance)
cd ~/diploma/distributed-rendering-diploma
screen -t cm_simple-600f20w-coarse -S cm_simple-600f20w-coarse -L -Logfile cm_simple-600f20w-coarse.log
RUST_LOG="debug" ./target/release/master --host 0.0.0.0 --port 9902 run-job --resultsDirectory ./blender-projects/01_simple-animation/results ./blender-projects/01_simple-animation/01-simple-animation_measuring_600f-20w_naive-coarse.toml

# 20 workers (spawned with `srun`)
cd ~/diploma/distributed-rendering-diploma
screen -t cw_simple-600f20w-coarse -S cw_simple-600f20w-coarse -L -Logfile cw_simple-600f20w-coarse.log
srun --job-name=cw-simple-600f20w-coarse --ntasks=20 --output=cw_simple-600f20w-coarse.workers.log --time=300 --mem=3G --cpus-per-task=4 --threads-per-core=1 -- singularity exec --bind /ceph/grid/home/sg7710/diploma/distributed-rendering-diploma/blender-projects --env RUST_LOG="debug" /ceph/grid/home/sg7710/diploma/blender-docker.sif /ceph/grid/home/sg7710/diploma/distributed-rendering-diploma/target/release/worker --masterServerHost nsc-login1 --masterServerPort 9902 --baseDirectory /ceph/grid/home/sg7710/diploma/distributed-rendering-diploma/ --blenderBinary /usr/bin/blender
```


### *Strategy: dynamic*
```bash
# Master (in a `screen` on the login1 instance)
cd ~/diploma/distributed-rendering-diploma
screen -t cm_simple-600f20w-dynamic -S cm_simple-600f20w-dynamic -L -Logfile cm_simple-600f20w-dynamic.log
RUST_LOG="debug" ./target/release/master --host 0.0.0.0 --port 9903 run-job --resultsDirectory ./blender-projects/01_simple-animation/results ./blender-projects/01_simple-animation/01-simple-animation_measuring_600f-20w_dynamic.toml

# 20 workers (spawned with `srun`)
cd ~/diploma/distributed-rendering-diploma
screen -t cw_simple-600f20w-dynamic -S cw_simple-600f20w-dynamic -L -Logfile cw_simple-600f20w-dynamic.log
srun --job-name=cw-simple-600f20w-dynamic --ntasks=20 --output=cw_simple-600f20w-dynamic.workers.log --time=300 --mem=3G --cpus-per-task=4 --threads-per-core=1 -- singularity exec --bind /ceph/grid/home/sg7710/diploma/distributed-rendering-diploma/blender-projects --env RUST_LOG="debug" /ceph/grid/home/sg7710/diploma/blender-docker.sif /ceph/grid/home/sg7710/diploma/distributed-rendering-diploma/target/release/worker --masterServerHost nsc-login1 --masterServerPort 9903 --baseDirectory /ceph/grid/home/sg7710/diploma/distributed-rendering-diploma/ --blenderBinary /usr/bin/blender
```
