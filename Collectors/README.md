### Building with Visual Studio

- Open `AnalyticsCollector.sln` in VS.
- Use `Build Solution` to build the source code.
- Add arguments in Debug properties for project.

### Parameters required for invoking collectors

* Here are the parameters required : 
   - alias (eg - user@microsoft.com for AzureDevops organization)
   - token (eg - PAT_TOKEN for AzureDevops organization)
   - clusterNameAndRegion for Azure Data explorer (eg - axexperiments.southeastasia ) 
   - AuthorityName for making Azure Data explorer AAD connection (eg - microsoft.com)
   - OrganizationName for Azure Devops (eg - mseng)
   - ProjectName for Azure Devops (eg - AzureDevops)

* Binaries for each assembly are produced in the
`AnalyticsCollectors/Collectors/AnalyticsCollector/bin/Debug` directory.
