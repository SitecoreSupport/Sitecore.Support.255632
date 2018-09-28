using Sitecore.Configuration;
using Sitecore.Data;
using Sitecore.Data.DataProviders;
using Sitecore.Data.DataProviders.Sql;
using Sitecore.Data.DataProviders.SqlServer;
using Sitecore.Data.Items;
using Sitecore.Data.SqlServer;
using Sitecore.Globalization;
using Sitecore.Workflows;
using System.Collections;
using System.Data;
using System.Data.SqlClient;

namespace Sitecore.Support.Data.SqlServer
{
  public class SqlServerDataProvider : Sitecore.Data.SqlServer.SqlServerDataProvider
  {
    private string ConnectionString
    {
      get
      {
        SqlServerDataApi sqlServerDataApi = base.Api as SqlServerDataApi;
        if (sqlServerDataApi == null)
        {
          return string.Empty;
        }
        return sqlServerDataApi.ConnectionString;
      }
    }

    public SqlServerDataProvider(string connectionString)
     : base(connectionString)
    {
    }

    public override DataUri[] GetItemsInWorkflowState(WorkflowInfo info, CallContext context)
    {
      using (DataProviderReader reader = this.Api.CreateReader("SELECT TOP ({2}maxVersionsToLoad{3}) {0}ItemId{1}, {0}Language{1}, {0}Version{1}\r\n          FROM {0}VersionedFields{1} WITH (NOLOCK) \r\n          WHERE {0}FieldId{1}={4}" + FieldIDs.WorkflowState.ToGuid().ToString("D") + "{4} \r\n          AND {0}Value{1}= {2}workflowStateFieldValue{3}\r\n          ORDER BY {0}Updated{1} desc", (object)"maxVersionsToLoad", (object)Settings.Workbox.SingleWorkflowStateVersionLoadThreshold, (object)"workflowStateFieldValue", (object)info.StateID))
      {
        ArrayList arrayList = new ArrayList();
        ArrayList arrayListNew = new ArrayList();
        int itemThreshold = Settings.Workbox.SingleWorkflowStateVersionLoadThreshold;
        while (reader.Read())
        {
          ID id = this.Api.GetId(0, reader);
          Language language = this.Api.GetLanguage(1, reader);
          Sitecore.Data.Version version = this.Api.GetVersion(2, reader);
          Item item = Context.ContentDatabase.GetItem(new DataUri(id, language, version));
          if (item != null)
            arrayList.Add((object)new DataUri(id, language, version));
        }

        int count = 0;
        var itemMaxSize = GetItemsSize(info);
        while (arrayList.Count < itemThreshold && ((count * itemThreshold) + itemThreshold) < itemMaxSize)
        {
          count++;
          var nextBatchItems = GetNextBatchItemsInWorkflowState(info, context, (count * itemThreshold));
          if (nextBatchItems.Count > 0)
          {
            foreach (var nextBacthItem in nextBatchItems)
            {
              arrayList.Add(nextBacthItem);
            }
          }
        }

        if (arrayList.Count > 0)
        {
          arrayListNew = (arrayList.Count <= itemThreshold) ? arrayList.GetRange(0, arrayList.Count) : arrayList.GetRange(0, itemThreshold);
        }
        return arrayListNew.ToArray(typeof(DataUri)) as DataUri[];
      }
    }

    protected virtual long GetItemsSize(WorkflowInfo info)
    {
      string sql = "SELECT COUNT(*) FROM [VersionedFields] WHERE [FieldId] = @fieldId AND [Value]= @value";
      using (SqlConnection connection = new SqlConnection(this.ConnectionString))
      {
        connection.Open();
        SqlCommand sqlCommand = new SqlCommand(sql, connection);
        sqlCommand.CommandTimeout = (int)this.CommandTimeout.TotalSeconds;
        sqlCommand.Parameters.AddWithValue("@fieldId", FieldIDs.WorkflowState.ToGuid().ToString("D"));
        sqlCommand.Parameters.AddWithValue("@value", info.StateID);
        using (SqlDataReader sqlDataReader = sqlCommand.ExecuteReader(CommandBehavior.SingleResult))
        {
          if (sqlDataReader.Read())
          {
            return SqlServerHelper.GetLong(sqlDataReader, 0);
          }
          return 0;
        }
      }
    }

    protected virtual ArrayList GetNextBatchItemsInWorkflowState(WorkflowInfo info, CallContext context, int next)
    {
      using (DataProviderReader reader = this.Api.CreateReader("SELECT {0}ItemId{1}, {0}Language{1}, {0}Version{1}\r\n          FROM {0}VersionedFields{1} WITH (NOLOCK) \r\n          WHERE {0}FieldId{1}={4}" + FieldIDs.WorkflowState.ToGuid().ToString("D") + "{4} \r\n          AND {0}Value{1}= {2}workflowStateFieldValue{3}\r\n          ORDER BY {0}Updated{1} desc\r\n          OFFSET " + next + " ROWS\r\n         FETCH NEXT ({2}maxVersionsToLoad{3}) ROWS ONLY", (object)"maxVersionsToLoad", (object)Settings.Workbox.SingleWorkflowStateVersionLoadThreshold, (object)"workflowStateFieldValue", (object)info.StateID))
      {
        ArrayList arrayList = new ArrayList();
        while (reader.Read())
        {
          ID id = this.Api.GetId(0, reader);
          Language language = this.Api.GetLanguage(1, reader);
          Sitecore.Data.Version version = this.Api.GetVersion(2, reader);
          Item item = Context.ContentDatabase.GetItem(new DataUri(id, language, version));
          if (item != null)
            arrayList.Add((object)new DataUri(id, language, version));
        }

        return arrayList;
      }
    }
  }
}