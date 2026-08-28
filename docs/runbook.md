# Operations and cost-safety runbook

## Current repository state

The repository has no GCP credentials, billing configuration, deploy workflow, running job, or created cloud resource. CI uses standard GitHub-hosted runners and only performs local tests and static Terraform validation.

Someone browsing or forking this public repository cannot charge the repository owner. A person can only deploy into a GCP project for which that person separately supplies authorized credentials and billing. Main-branch protection controls code changes; it does not itself stop cloud deployment. The mechanical safeguards in code and CI address that risk.

## Local validation

```bash
python -m pip install -r requirements-lock.txt
python -m pytest -q
python -m ruff check pipeline simulator tests
python -m ruff format --check pipeline simulator tests
python -m pip check

cd infra/terraform
terraform fmt -recursive -check
terraform init -backend=false
terraform validate
```

These commands do not authenticate or create resources. Do not set application-default credentials for this validation.

## Deliberate cloud deployment reference — not executed

Cloud use requires separate authorization. Before it is considered:

1. confirm the intended GCP project and billing owner;
2. create a small budget and billing alerts (alerts notify; they are not a hard spending cap);
3. obtain scoped credentials outside the repository;
4. review a saved Terraform plan;
5. set both durable Terraform gates: `deployment_enabled=true` and `deployment_confirmation="DEPLOY"`, and keep both values unchanged for as long as Terraform manages the deployment;
6. apply only after peer approval;
7. launch Dataflow with `--runner=DataflowRunner`, every required project/region/subscription/table/location/service-account option, `--requirements_file=requirements-runtime-lock.txt`, `--setup_file=./setup.py`, and `--confirm_cloud_run=DATAFLOW`;
8. record the job ID and assign an operator responsible for stopping it.

The repository intentionally supplies no executable deployment workflow. Default Terraform apply manages zero resources. The example tfvars keeps both gates closed. Resource counts depend only on `deployment_enabled`; clearing only the confirmation token fails validation and cannot plan resource deletion. The artifact bucket and BigQuery dataset also use `prevent_destroy`, while BigQuery tables retain provider deletion protection.

## Stop and cleanup

Dataflow streaming jobs incur charges while running. Stop the job first using the Google Cloud console or an explicitly authorized `gcloud dataflow jobs cancel JOB_ID --region REGION`, then verify the job has reached a terminal state.

After data retention decisions:

1. export or retain required BigQuery/Pub/Sub data;
2. remove table deletion protection and the bucket/dataset `prevent_destroy` blocks only through a reviewed code change if deletion is truly intended;
3. review a Terraform destroy plan;
4. destroy intentionally and verify Dataflow, Pub/Sub, BigQuery, GCS, monitoring, and service-account resources are gone;
5. disable unused APIs and remove temporary credentials only after confirming no shared dependency.

Never close `deployment_enabled` as a routine post-apply action: it expresses durable desired state, not a momentary confirmation. `prevent_destroy`, `force_destroy=false`, dataset content preservation, bucket soft delete, and table deletion protection intentionally make destructive cleanup require a reviewed code change rather than a one-command surprise.

## Incident triage order

1. Confirm Dataflow job state and recent errors.
2. Check subscription backlog and oldest-unacked age.
3. Compare Beam received/valid/invalid/rule/velocity metrics.
4. Inspect BigQuery and quarantine sink errors/quotas.
5. Check watermark lag, late panes, and hot keys.
6. Correct the fault, then restart against the durable subscription; do not republish blindly.

No current implementation can guarantee against a person who obtains credentials and deliberately edits/removes safeguards. The repository does prevent default and CI-driven accidental deployment in its reviewed state.
