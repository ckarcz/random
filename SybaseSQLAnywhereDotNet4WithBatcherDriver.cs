using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Text;

    using NHibernate;
    using NHibernate.AdoNet;
    using NHibernate.AdoNet.Util;
    using NHibernate.Driver;
    using NHibernate.Engine;
    using NHibernate.Exceptions;

    public class SybaseSQLAnywhereDotNet4WithBatcherDriver : SybaseSQLAnywhereDotNet4Driver, IEmbeddedBatcherFactoryProvider
    {
        public Type BatcherFactoryClass
        {
            get
            {
                return typeof(SybaseAnywhereClientBatchingBatcherFactory);
            }
        }
    }

    public class SybaseAnywhereClientBatchingBatcherFactory : IBatcherFactory
    {
        public SybaseAnywhereClientBatchingBatcherFactory()
        {
        }

        public virtual IBatcher CreateBatcher(ConnectionManager connectionManager, IInterceptor interceptor)
        {
            return new SybaseAnywhereClientBatchingBatcher(connectionManager, interceptor);
        }
    }

    public class SybaseAnywhereClientBatchingBatcher : AbstractBatcher
    {
        private int _batchSize;

        private int _countOfCommands;

        private int _totalExpectedRowsAffected;

        private IDbCommand _currentBatch;

        private IDictionary<string, List<object>> _parameterValueListHashTable;

        private IDictionary<string, bool> _parameterIsAllNullsHashTable;

        private StringBuilder _currentBatchCommandsLog;

        public override int BatchSize
        {
            get
            {
                return this._batchSize;
            }
            set
            {
                this._batchSize = value;
            }
        }

        protected override int CountOfStatementsInCurrentBatch
        {
            get
            {
                return this._countOfCommands;
            }
        }

        public SybaseAnywhereClientBatchingBatcher(ConnectionManager connectionManager, IInterceptor interceptor)
            : base(connectionManager, interceptor)
        {
            this._batchSize = base.Factory.Settings.AdoBatchSize;
            this._currentBatchCommandsLog = (new StringBuilder()).AppendLine("Batch commands:");
        }

        public override void AddToBatch(IExpectation expectation)
        {
            List<object> item;
            bool flag = true;
            SybaseAnywhereClientBatchingBatcher expectedRowCount = this;
            expectedRowCount._totalExpectedRowsAffected = expectedRowCount._totalExpectedRowsAffected + expectation.ExpectedRowCount;
            string commandLineWithParameters = null;
            SqlStatementLogger sqlStatementLogger = base.Factory.Settings.SqlStatementLogger;
            if (sqlStatementLogger.IsDebugEnabled || AbstractBatcher.Log.IsDebugEnabled)
            {
                commandLineWithParameters = sqlStatementLogger.GetCommandLineWithParameters(base.CurrentCommand);
                FormatStyle formatStyle = sqlStatementLogger.DetermineActualStyle(FormatStyle.Basic);
                commandLineWithParameters = formatStyle.Formatter.Format(commandLineWithParameters);
                this._currentBatchCommandsLog.Append("command ").Append(this._countOfCommands).Append(":").AppendLine(commandLineWithParameters);
            }
            if (AbstractBatcher.Log.IsDebugEnabled)
            {
                AbstractBatcher.Log.Debug(string.Concat("Adding to batch:", commandLineWithParameters));
            }
            if (this._currentBatch != null)
            {
                flag = false;
            }
            else
            {
                this._currentBatch = base.CurrentCommand;
                this._parameterValueListHashTable = new Dictionary<string, List<object>>();
                this._parameterIsAllNullsHashTable = new Dictionary<string, bool>();
            }

            for (int i = 0; i < base.CurrentCommand.Parameters.Count; i++)
            {
                var parameter = (IDataParameter)base.CurrentCommand.Parameters[i];

                if (!flag)
                {
                    item = this._parameterValueListHashTable[parameter.ParameterName];
                }
                else
                {
                    item = new List<object>();
                    this._parameterValueListHashTable.Add(parameter.ParameterName, item);
                    this._parameterIsAllNullsHashTable.Add(parameter.ParameterName, true);
                }
                if (parameter.Value != DBNull.Value)
                {
                    this._parameterIsAllNullsHashTable[parameter.ParameterName] = false;
                }

                item.Add(parameter.Value);
            }

            SybaseAnywhereClientBatchingBatcher batcher = this;
            batcher._countOfCommands = batcher._countOfCommands + 1;
            if (this._countOfCommands >= this._batchSize)
            {
                base.ExecuteBatchWithTiming(this._currentBatch);
            }
        }

        protected override void DoExecuteBatch(IDbCommand ps)
        {
            int num = 0;
            if (this._currentBatch != null)
            {
                this._countOfCommands = 0;
                AbstractBatcher.Log.Info("Executing batch");
                base.CheckReaders();
                base.Prepare(this._currentBatch);
                if (base.Factory.Settings.SqlStatementLogger.IsDebugEnabled)
                {
                    base.Factory.Settings.SqlStatementLogger.LogBatchCommand(this._currentBatchCommandsLog.ToString());
                    this._currentBatchCommandsLog = (new StringBuilder()).AppendLine("Batch commands:");
                }

                if (this._parameterValueListHashTable.Count != 0 && ps.Parameters.Count != 0)
                {
                    var firstParamName = ((IDataParameter)ps.Parameters[0]).ParameterName;
                    var paramValueList = this._parameterValueListHashTable[firstParamName];

                    // Save each row
                    for (int i = 0; i < paramValueList.Count; i++)
                    {
                        try
                        {
                            var command = (IDbCommand)ps;

                            for (int j = 0; j < command.Parameters.Count; j++)
                            {
                                var parameter = (IDataParameter)command.Parameters[j];
                                List<object> item = this._parameterValueListHashTable[parameter.ParameterName];
                                parameter.Value = item[i];
                            }

                            command.Prepare();
                            var rowsEffected = command.ExecuteNonQuery();

                            num += rowsEffected;
                        }
                        catch (DbException dbException)
                        {
                            throw ADOExceptionHelper.Convert(
                                base.Factory.SQLExceptionConverter,
                                dbException,
                                "could not execute batch command.");
                        }
                    }

                    Expectations.VerifyOutcomeBatched(this._totalExpectedRowsAffected, num);
                }

                this._totalExpectedRowsAffected = 0;
                this._currentBatch = null;
                this._parameterValueListHashTable = null;
            }
        }

    }
