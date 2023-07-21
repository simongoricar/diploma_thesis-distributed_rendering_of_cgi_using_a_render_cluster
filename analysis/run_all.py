import efficiency
import job_duration
import job_tail_delay
import results_statistics
import speedup
import worker_latency
import worker_utilization
from core.parser import load_traces_from_default_path


def main():
    traces = load_traces_from_default_path()

    results_statistics.main_analyze(traces)

    job_duration.main_plot(traces)
    speedup.main_plot(traces)
    efficiency.main_plot(traces)
    worker_utilization.main_plot(traces)
    worker_latency.main_plot(traces)
    job_tail_delay.main_plot(traces)

if __name__ == '__main__':
    main()
