
# How this project employs Terraform to provision cloud infrastructure

<br><br>


## Modules
The Terraform code includes two modules: 
 - a root module <br>
 		This provisions infrastructure that is not repeated (for example the code S3 bucket, which contains the zipped lambda handlers and the zipped layer) and calls the child module several times. 
 - a child module <br>
		This provisions (in successive invocations) infrastructure that is repeated.
 <br><br><br>



## Explanation
### Conceptual breakdown of the project infrastructure 

 We can arbitrarily break down the cloud infrastructure of the project into these three sections: <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;section (1):&nbsp;&nbsp;&nbsp;&nbsp;The extract lambda plus associated infrastructure, and the ingestion bucket plus associated infrastructure <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;section (2):&nbsp;&nbsp;&nbsp;&nbsp;The transform lambda plus associated infrastructure, and the processed bucket plus associated infrastructure <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;section (3):&nbsp;&nbsp;&nbsp;&nbsp;The load lambda plus associated infrastructure <br>
<br> <br> 
  
 
 
 ### Invocations of the child module in the root module:
  
 This project's root module calls the child module three times to provision sections (1), (2) and (3) (listed above).  <br>
 
 The child module conditionally provisions:<br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;(a)&nbsp;&nbsp;&nbsp;&nbsp;a lambda function and associated infrastructure,  <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;(b)&nbsp;&nbsp;&nbsp;&nbsp;an s3 bucket and associated infrastructure<br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
<br> 

The root module's <br> 

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; first invocation of the child module provisions section 1 above (the extract lambda (a) and the ingestion bucket (b)).

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; second invocation of the child module provisions section 2 above (the transform lambda (a) and the processed bucket (b)).

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; third invocation of the child module provisions section 3 above (the load lambda (a) only).
 
 <br><br><br>



