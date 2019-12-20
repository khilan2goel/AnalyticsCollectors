### Building with Visual Studio

- Open `AnalyticsCollector.sln` in VS.
- Use `Build Solution` to build the source code.
- Add arguments in Debug properties for project.

* Binaries for each assembly are produced in the
`AnalyticsCollectors/Collectors/AnalyticsCollector/bin/Debug` directory. We can invoke exe generated in this folder using command line arguments as well.

### Directly using artifacts to run collectors

- Download artifacts from [here](https://github.com/khilan2goel/AnalyticsCollectors/suites/368469212/artifacts/780984)
- Run below command from windows command prompt:  
`AnalyticsCollector.exe alias token connectionURI aadTenantIdOrTenantName OrganizationName ProjectName`


#### NOTE: 
Sometimes, Azure Data explorer connection is cached, please make sure to delete token from `%APPDATA%\Kusto\tokenCache.data` and try again.

### Parameters required for invoking collectors

* Here are the parameters required : 
   - alias (eg - user@microsoft.com for AzureDevops organization)
   - token (eg - PAT_TOKEN for AzureDevops organization)
   - connection uri for Azure Data explorer (eg - axexperiments.southeastasia.kusto.windows.net) 
   - aadTenantIdOrTenantName for making Azure Data explorer AAD connection (either provide AAD tenant GUID or AAD domain name. eg - microsoft.com)
   - OrganizationName for Azure Devops from where to pull data (eg - mseng)
   - ProjectName for Azure Devops from where to pull data (eg - AzureDevops)
