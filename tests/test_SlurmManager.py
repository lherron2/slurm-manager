import unittest
import SlurmManager

class TestSlurmManager(unittest.TestCase):

    def setUp(self):
        self.manager = SlurmManager()

    def test_generate_slurm_args(self):
        result = self.manager.generate_slurm_args()
        expected_defaults = [
            "#SBATCH --partition=default",
            "#SBATCH --ntasks=1",
            "#SBATCH --cpus-per-task=1",
            "#SBATCH --mem=2G",
            "#SBATCH --time=1:00:00",
            "#SBATCH --job-name=python_job"
        ]
        for line in expected_defaults:
            self.assertIn(line, result)

    def test_is_slurm_available(self):
        result = self.manager.is_slurm_available()
        self.assertIsInstance(result, bool)

    def test_submit_script(self):
        # If SLURM is available, check for "Submitted batch job" in the output.
        # Otherwise, expect the Python script output.
        result = self.manager.submit_script('''print("Hello from direct Python execution!")''')
        if self.manager.slurm_available:
            self.assertIn("Submitted batch job", result)
        else:
            self.assertIn("Hello from direct Python execution!", result)

if __name__ == "__main__":
    unittest.main()

