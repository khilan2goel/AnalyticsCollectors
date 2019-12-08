-- Example for running template. Use PowerShell and install Azure CLI

-- Login into Azure RM account
> Login-AzureRmAccount

-- Select required azure subscription
> Select-AzureRmSubscription -SubscriptionId "SubscriptionId"

-- Run below commandlet

> $parameters = @{
"cluster_name" = "axexperiments2"
"location" =  "Southeast Asia"
"skuName" = "Dev(No SLA)_Standard_D11_v2"
"tier" = "Basic"
}

> $deployment = New-AzureRmResourceGroupDeployment -ResourceGroupName "ResourceGroupName" -TemplateFile "TemplateFilePath\AzureDataExplorer.json" -TemplateParameterObject $parameters