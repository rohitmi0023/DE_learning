### **GCP IAM Explained Like You're 10 (With Simple Analogies)** 🍎🏰

GCP- Ek Giant Castle ki tarah hei jisme important stuffs hei like my storage, compute instances, etc. Castle ko unwanted attackers se protect karne ke liye kuch security design hote hei jaise ki- keys(permissions), job titles(roles) and people/robots(principals).
1. Principals: Identification 👨👩👧👦
	1. User Accounts -> individuals(me@gmail..com)
	2. Service Accounts -> Robots(beam-runner@project.iam.gserviceaccount.com)
	3. Groups
	4. Domains
2. Permissions- Castle ke andar kya karna allowed hei like delete files from storage etc.
3. Roles 👑: Job Titles in Castle. Jaise ki Storage admin(roles/storage.admin), Service Account User(roles/iam.serviceAccountUser)
4. Policies: The Castle’s Rulebook 📜. Policy is a document that says: "Alice gets the storage Admin role for this Bucket". Two Types- Project level policy, bucket level policy.
5. Service Accounts:  Robot Workers 🤖
## **Final Cheat Sheet** 📋

| Concept             | Simple Explanation                 | Example                                          |
| ------------------- | ---------------------------------- | ------------------------------------------------ |
| **Principal**       | Who needs access?                  | `user:alice@company.com`                         |
| **Permission**      | Tiny ability                       | `storage.objects.get`                            |
| **Role**            | Job title (group of permissions)   | `roles/storage.admin`                            |
| **Policy**          | Rule assigning roles to principals | _"Alice is Storage Admin"_                       |
| **Service Account** | Robot worker                       | `dataflow-robot@project.iam.gserviceaccount.com` |

Project Access
Principal-> prod-playground-sa@apache-beam-testing.iam.gserviceaccount.com
Role-> Storage Admin

DataFlow Execution Checklist for Apache Beam on GCP
1. GCP Setup
    * Dataflow API Enabled
    * Compute Engine API Enabled
    * Cloud Storage API Enabled
    * BigQuery API Enabled(if used)
2. Service Account Permissions
	    * Dataflow Service Account has:
	        - roles/dataflow.worker
	        - roles/storage.objectAdmin
	        - roles/bigquery.dataEditor
3. IAM Permissions
	    * roles/dataflow.admin
	    * roles/iam.serviceAccountUser
	    * roles/storage.admin
	    * roles/bigquery.dataEditor
4. Project Principals
	    * prod-playground-sa@apache-beam-testing.iam.gserviceaccount.com -> Storage Admin
5. Storage Buckets
    * Input Bucket Exists: gs://apache_beam113
    * Input file exists: kinglear.txt
    * Temp bucket exists: gs://my-bucket/temp
    * Staging bucket exists: gs://my-bucket/staging\
6. Pipeline Configuration
    * Correct runner: DataflowRunner
    * Valid GCP project ID: bigquerylearning-464918
    * Unique job name (changes with each run)
    * Region specified (e.g., us-central1)
    * Temp and staging locations exist
7. Code Validations
    * All required imports present
    * GCS paths use gs:// scheme (not HTTP)
    * Output is a prefix, not full path
    * Correct option names (e.g., temp_location not temp_locations)
    * No syntax errors
