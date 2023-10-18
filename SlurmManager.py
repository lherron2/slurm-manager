import subprocess
import tempfile


class SlurmManager:
    def __init__(self):
        pass

    def submit_script(self, python_script, preamble=None, job_name="python_job"):
        """
        Submit a python script to the cluster using SLURM.

        :param python_script: The python script to run (as a multi-line string)
        :param preamble: Additional lines to be executed before the python script (e.g., module loads)
        :param job_name: Name of the job for SLURM
        :return: Output from the sbatch command.
        """
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".sh", delete=True
        ) as tmpfile:
            tmpfile.write("#!/bin/bash\n")
            tmpfile.write(f"#SBATCH --job-name={job_name}\n")
            tmpfile.write("#SBATCH --output=slurm_%j.out\n")
            tmpfile.write("#SBATCH --error=slurm_%j.err\n")

            # Add the preamble if provided
            if preamble:
                tmpfile.write(preamble + "\n")

            tmpfile.write("\n")
            tmpfile.write(f"python - <<EOF\n{python_script}\nEOF\n")

            # Flush contents to ensure they're written to disk before submission
            tmpfile.flush()

            # Submit the job script
            result = subprocess.run(
                ["sbatch", tmpfile.name], capture_output=True, text=True
            )
            return result.stdout

    def count_jobs(self, state="RUNNING"):
        """
        Count the number of jobs in a given state.

        :param state: State of the jobs to count (e.g., "RUNNING", "PENDING"). Default is "RUNNING".
        :return: Number of jobs in the given state.
        """
        result = subprocess.run(
            ["squeue", "-h", "-t", state, "-o", "%i"], capture_output=True, text=True
        )
        jobs = result.stdout.splitlines()
        return len(jobs)

    def cancel_jobs_by_name(self, job_name):
        """
        Cancel jobs by name.

        :param job_name: Name of the job(s) to cancel.
        :return: Output from the cancel command.
        """
        result = subprocess.run(
            ["scancel", "--name", job_name], capture_output=True, text=True
        )
        return result.stdout

    def view_queue(self):
        """
        View the SLURM queue.

        :return: Output from the squeue command.
        """
        result = subprocess.run(["squeue"], capture_output=True, text=True)
        return result.stdout

    def count_jobs_by_name(self, job_name, state=None):
        """
        Count the number of jobs filtered by a given name.

        :param job_name: Name of the job to count.
        :param state: Optional state to further filter by (e.g., "RUNNING", "PENDING").
        :return: Number of jobs with the given name (and state, if provided).
        """
        cmd = ["squeue", "-h", "-n", job_name, "-o", "%i"]
        if state:
            cmd.extend(["-t", state])
        result = subprocess.run(cmd, capture_output=True, text=True)
        jobs = result.stdout.splitlines()
        return len(jobs)

    def is_slurm_available(self):
        """
        Check if SLURM is available on the system.

        :return: True if SLURM is available, False otherwise.
        """
        try:
            result = subprocess.run(
                ["sbatch", "--version"], capture_output=True, text=True, check=True
            )
            return "sbatch" in result.stdout
        except Exception:
            return False
