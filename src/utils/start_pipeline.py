import wandb
import sys

if len(sys.argv) < 2:
    print("Usage: python start_pipeline.py <run_name>")
    sys.exit(1)

run_name = sys.argv[1]
wandb.init(project="my_ml_project", name=run_name)
print(wandb.run.id)
wandb.finish()
