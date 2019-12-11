### Steps to provision Azure Data explorer in your subscription using ARM template.

Below commandlets will run on PowerShell.
Make sure to install Azure CLI

## Login into Azure RM account
- Login-AzureRmAccount

## Select required azure subscription
- Select-AzureRmSubscription -SubscriptionId "SubscriptionId"

## Create a resource group into which the resources will be deployed
- New-AzureRmResourceGroup -Name "name" -Location "location"

## Run below command to provision Azure Data explorer using ARM template 

- $parameters = @{  
"cluster_name" = "axexperiments2"  
"location" =  "Southeast Asia"  
"skuName" = "Dev(No SLA)_Standard_D11_v2"  
"tier" = "Basic"  
}

- $deployment = New-AzureRmResourceGroupDeployment -ResourceGroupName "ResourceGroupName" -TemplateFile "TemplateFilePath\AzureDataExplorer.json" -TemplateParameterObject $parameters
