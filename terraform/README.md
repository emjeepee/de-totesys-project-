
# Use of Terraform to provision AWS cloud infrastructure
<br><br>

This document contains:  <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; 
– A conceptual breakdown of the infrastructure <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; 
– An overview of the modules<br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; 
– A description of the modules in detail<br>

<br><br>


## Conceptual breakdown of the infrastructure 

 We can arbitrarily split the cloud infrastructure of the project into four <br>
 sections: <br> <br>
*section 1*  <br>
The provider, the code and backend buckets, the EventBridge <br>
Scheduler, associated infrastructure <br> <br>
*section 2* <br>
The extract lambda plus associated infrastructure, and the <br>
ingestion bucket plus associated infrastructure <br> <br>
*section 3* <br>
The transform lambda plus associated infrastructure, and the <br>
processed bucket plus associated infrastructure <br> <br>
*section 4* <br>
The load lambda plus associated infrastructure <br>
<br> <br> 
 This project employs two Terraform modules:   
   1) a root module to provision section (section 1) <br> 
   2) a child module that the root module invokes three times to provision <br>(sections 2, 3 and 4)

<br><br><br>
 
 ## Overview of the modules:
  
 The root module provisions section 1.  <br>
 The root module calls the child module three times to provision sections 2, 3<br>
 and 4.  <br>
 
 The child module conditionally provisions:<br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;(a)&nbsp;&nbsp;&nbsp;&nbsp;a lambda function and associated infrastructure,  <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;(b)&nbsp;&nbsp;&nbsp;&nbsp;an s3 bucket and associated infrastructure<br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
<br> 

The root module's <br> 
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; 
first invocation of the child module provisions section 2 (the extract lambda  <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; 
and the ingestion bucket);  <br><br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; 
second invocation of the child module provisions section 3 (the transform  <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; 
lambda and the processed bucket);  <br><br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; 
third invocation of the child module provisions section 4 (the load lambda  <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; 
only). <br>
 
 <br><br><br>



## The modules in detail
### Root module
The root module provisions the following non-repeated infrastructure: <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;The provider (AWS) <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;The EventBridge Scheduler (to trigger the extract lambda at some frequency)  <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;An execution role for the Scheduler  <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;A policy to allow the scheduler to trigger a lambda function <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;A policy attachment for the scheduler  <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;A backend S3 bucket to hold the state file  <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;An S3 bucket to store the three zipped lambda handlers and shared zipped layer  <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;A policy to allow AWS Lambda service to read the bucket that contains the zipped files  <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;The loading of the zipped lambdas into the code bucket  <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;The loading of the zipped shared layer into the code bucket  <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;The lambda layer version  <br> <br>
The root module also invokes the child module thrice, passing in boolean variables each time to provision some resources of the child module and to avoid provisioning others.
<br>
<br>

### Child module
The child module provisions infrastructure that is repeated. <br><br>
The first invocation of this module by the root module provisions: <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;The extract lambda function<br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;An execution role for the extract lambda function<br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;A role policy for writing to the ingestion bucket <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;An attachment for the policy <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;The ingestion S3 bucket <br><br>

The second invocation of this module by the root module provisions:  <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;The transform lambda function<br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;An execution role for the transform lambda <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;A role policy for reading from the ingestion bucket <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;The attachment for that policy <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;A policy to write to the processed bucket <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;An attachment for that policy <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;The processed S3 bucket <br><br>

The third invocation of this module by the root module provisions:    <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;The load lambda function <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;A policy to let the lambda read from the processed bucket <br>

 <br><br><br>

## Environment variables and secrets:  <br>
The code in the lambda handlers of this project employs secret values that  <br>
reside in environment variables. <br> 

This project employs Terraform variables to help keep them secret.  <br> 

For example the environment variable AWS_INGEST_BUCKET, set in development on a  <br>
local machine, contains the name of the ingestion bucket. In production cloud  <br>
service AWS Lambda must also make environment variable AWS_INGEST_BUCKET <br> available to code in the lambda handlers.  <br>

This project keeps the values of environment variables secret by using this <br>
code in file **main.tf** in directory child-module:  <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;


```hcl
resource "aws_lambda_function" "mod_lambda" {

  environment {
    variables = local.common_env_vars
              }
                                            }                  
```

<br>

and the locals block in file **main.tf** of directory child-module contains <br> 
this code: <br>

```hcl
locals {
  common_env_vars = {
    AWS_INGEST_BUCKET  = var.AWS_INGEST_BUCKET 
	etc 
       				}
	    }	
```
<br>

Additionally in development this project sets the value of environment variable 
TF_VAR_AWS_INGEST_BUCKET to the name of the ingestion bucket.<br>

On running this in the command line:<br>
```terraform apply``` <br>
Terraform sets the value of *terraform* variable var.AWS_INGEST_BUCKET to the <br> 
value of TF_VAR_AWS_INGEST_BUCKET, ie the string that is the name of the <br>
ingestion bucket. <br>

The same applies for other environment variables that hold secrets. <br>
 <br>
  <br>

### CI/CD 
This project uses GitHub Actions for CI/CD.<br>

The workflow.yml employs lines such as the following two to ensure that the <br>
correct environment variables are available for running all tests and for <br>
ensuring that cloud service AWS Lambda makes secret values available for the <br> 
lambda handlers:

1) to ensure that secret values are available for tests -<br>
```AWS_INGEST_BUCKET: '${{ secrets.AWS_INGEST_BUCKET }}' ```   <br>

2) to ensure that terraform sets environment variables for AWS Lambda to the <br>
correct secret values -<br>
```TF_VAR_AWS_INGEST_BUCKET: '${{ secrets.AWS_INGEST_BUCKET }}'``` <br>

<br>
In retrospect this project should have:<br>
a) set only environment variable such as TF_VAR_AWS_INGEST_BUCKET in <br>
development and should have avoided using environment variable such as <br> AWS_INGEST_BUCKET at all.<br>
b) ensured that the code of the lambda handlers accessed secret values by <br> 
reading environment variables such as TF_VAR_AWS_INGEST_BUCKET. <br>
c) setting only environment variables such as TF_VAR_AWS_INGEST_BUCKET in <br> 
GitHub Actions, like this <br>

```TF_VAR_AWS_INGEST_BUCKET: '${{ secrets.AWS_INGEST_BUCKET }}'``` ,
<br>
ie avoiding setting ```AWS_INGEST_BUCKET``` at all.
