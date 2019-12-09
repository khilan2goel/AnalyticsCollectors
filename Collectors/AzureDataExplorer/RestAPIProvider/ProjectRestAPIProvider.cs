using Microsoft.TeamFoundation.Core.WebApi;

namespace AzureDataExplorer
{
    public class ProjectRestAPIProvider
    {
        private ProjectHttpClient projectHttpClient;
        private string projectName;

        public ProjectRestAPIProvider(ProjectHttpClient projectHttpClient, string projectName)
        {
            this.projectHttpClient = projectHttpClient;
            this.projectName = projectName;
        }

        public TeamProject GetProjectInfo(string projectName)
        {
            return this.projectHttpClient.GetProject(projectName).Result;
        }
    }
}