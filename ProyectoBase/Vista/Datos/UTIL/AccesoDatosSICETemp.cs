using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Reflection;

namespace DATOS.UTIL
{
    public class AccesoDatosSICETemp : IDisposable
    {
        //Contiene la transacción que se ejecutará en la base de datos
        private SqlTransaction sqlTransaccion;
        //Variable para la conexión a la base de datos.
        private SqlConnection sqlConexion;
        //Contiene el nombre y valor del campo llave de la tabla sobre la 
        private CampoValor cvLlaveTabla;
        //                                   que se ejecutará una operación ABC.
        //Indica si el tipo de dato de la llave es cadena o número
        private ENUM_TIPO_ID enumTipoDatoLlave;
        //                                   con el fin de armar una condición en una sentencia SQL
        //Contiene el nombre de la tabla sobre la cual se realizará una operación ABC.
        private string strNombreTabla;

        //Variable para detectar llamadas redundantes
        bool disposed = false;

        //Indica si se utilizará o no transacción al realizar una operación sobre la base de datos
        private bool blnUsarTransaccion = false;

        //Variables para las propiedades
        private string _Consulta;

        private InfoStoredProcedureSICETemp _StoreProcedure;
        /// <summary>
        /// Propiedad que permite configurar el stored procedure que se va a ejecutar en la base de datos
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public InfoStoredProcedureSICETemp StoreProcedure
        {
            get
            {
                return _StoreProcedure;
            }
            set
            {
                _StoreProcedure = value;
            }
        }

        /// <summary>
        /// propiedad que permite indicar una sentencia SQL que se ejecutará sobre la base de datos.
        /// </summary>
        /// <value></value>
        /// <remarks></remarks>
        public string Consulta
        {
            set { _Consulta = value; }
        }

        /// <summary>
        /// Devuele un valor true si se hace uso de transacciones, de lo contrario false.
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public bool UsaTransaccion
        {
            get { return blnUsarTransaccion; }
        }


        /// <summary>
        /// Constructor de la clase de acceso a datos, aqui se establece la 
        /// conexión con la BD
        /// </summary>
        /// <param name="conTransaccion">(TRUE) Indica que las operaciones a realizar con 
        /// la instancia requieren el uso de una transacción.
        /// (FALSE) Indica que las operaciones no requieren transacciones.
        /// </param>
        /// <remarks></remarks>
        public AccesoDatosSICETemp(bool conTransaccion)
        {
            try
            {
                //Construye la cadena de conexion, con los parámetros de configuración
                sqlConexion = new SqlConnection("Server=" + ConfiguracionServidorSICETemp.G_SERVER + ";" +
                    "Database=" + ConfiguracionServidorSICETemp.G_DATABASE + ";" +
                     "User ID=" + ConfiguracionServidorSICETemp.G_USER_ID + ";" +
                    "Password=" + ConfiguracionServidorSICETemp.G_USER_PWD + ";");

                //Carga la variable local para indicar si se utilizará o no transacción
                //para el acceso a la base de datos.
                blnUsarTransaccion = conTransaccion;

                //Instancia del procedimiento almacenado para si es necesario utilizarlo
                _StoreProcedure = new InfoStoredProcedureSICETemp(this);

                //Abre la conexión a la base de datos
                AbrirConexion();
            }
            catch (Exception ExceptionErr)
            {
                throw new System.Exception(ExceptionErr.Message, ExceptionErr.InnerException);
            }

        }

        /// <summary>
        /// Método que permite probar si los valores de configuración (recibidos como parámetros)
        /// para conectarse a la base de datos (BD), permiten abrir la conexión exitosamente o no
        /// </summary>
        /// <param name="Server">Nombre o IP del servidor que contiene la BD</param>
        /// <param name="UserID">Nombre del usuario para acceder al servidor de BD</param>
        /// <param name="Password">Contraseña del usuario del servidor de BD</param>
        /// <returns>-Devuelve True si pudo establecerce la conexión a la BD
        /// -Devuelve False si no se pudo establecer la conexión a la BD</returns>
        /// <remarks></remarks>
        public static bool ProbarConexion(string Server, string UserID, string Password)
        {
            bool functionReturnValue = false;
            //Se establece una variable para la conexión adicional a la de la clase
            //ya que solamente es una prueba de los parámetros de conexión
            SqlConnection objConexion = default(SqlConnection);

            string strCadenaConexion = null;
            try
            {
                //Configuración y prueba de la conexión
                strCadenaConexion = "Server=" + Server + ";" + "Database=" + ConfiguracionServidorSICETemp.G_DATABASE + ";" + "User ID=" + UserID + ";" + "Password=" + Password;

                objConexion = new SqlConnection(strCadenaConexion);
                objConexion.Open();
                functionReturnValue = true;
                objConexion.Close();
                objConexion.Dispose();
                objConexion = null;

            }
            catch (Exception)
            {
                functionReturnValue = false;
            }
            return functionReturnValue;
        }

        /// <summary>
        /// Establece la conexión con la base de datos
        /// </summary>
        /// <remarks>Aqui se establece si la conexión va a incluir el manejo de una transacción.</remarks>
        private void AbrirConexion()
        {
            try
            {
                //Verifica que la conexión esté cerrada para que sea abierta
                if (sqlConexion.State == ConnectionState.Closed)
                {
                    sqlConexion.Open();
                    //Verifica si se establecerá la conexión con manejo de transacciones.
                    if (blnUsarTransaccion)
                    {
                        sqlTransaccion = sqlConexion.BeginTransaction(IsolationLevel.ReadCommitted);
                    }
                }

            }
            catch (SqlException OleDbExceptionErr)
            {
                throw new System.Exception(OleDbExceptionErr.Number + "-" + OleDbExceptionErr.Message, OleDbExceptionErr.InnerException);
            }
            catch (InvalidOperationException InvalidOperationExceptionErr)
            {
                throw new System.Exception(InvalidOperationExceptionErr.Message, InvalidOperationExceptionErr.InnerException);
            }
        }

        /// <summary>
        /// Cierra la conexión si es que está abierta.Además, 
        /// si la conexión se abrió con el uso de transacciones,
        /// la transacción se compromete antes de cerrarla.
        /// </summary>
        /// <remarks></remarks>
        public void CerrarConexion()
        {
            if (sqlConexion.State == ConnectionState.Open)
            {
                if (blnUsarTransaccion)
                    sqlTransaccion.Commit();
                sqlConexion.Close();
            }
        }

        /// <summary>
        /// Deshace la transacción activa y cierra la conexión.
        /// </summary>
        /// <remarks></remarks>
        public void rollBackTransaction()
        {
            if (!blnUsarTransaccion)
                return;
            if (sqlConexion.State == ConnectionState.Open)
            {
                sqlTransaccion.Rollback();
                sqlConexion.Close();
            }
        }

        /// <summary>
        /// Compromete la transacción activa y 
        /// cierra la conexión.
        /// </summary>
        /// <remarks></remarks>
        private void commitTransaction()
        {
            if (!blnUsarTransaccion)
                return;
            if (sqlConexion.State == ConnectionState.Open)
            {
                sqlTransaccion.Commit();
                sqlConexion.Close();
            }
        }

        /// <summary>
        /// Inicializa el comando que se manda como parámetro
        /// de acuerdo a la consulta que esté configurada actualmente
        /// y de acuerdo también a si se ha configurado un stored procedure.
        /// </summary>
        /// <returns>Comando inicializado con la consulta y la conexión recibidad como parámetro</returns>
        private SqlCommand inicializarCommand()
        {
            try
            {
                //Inicializa el comando con el valor actual de la consulta (propiedad)
                //y de acuerdo a la conexión activa
                SqlCommand sqlComando = new SqlCommand(_Consulta, sqlConexion);
                if (blnUsarTransaccion)
                    sqlComando.Transaction = sqlTransaccion;

                //Verifica si está configurado el procedimiento almacenado,
                //ya que de ser así el tipo de comando debe cambiarse
                if ((_StoreProcedure.Nombre != null))
                {
                    sqlComando.CommandText = _StoreProcedure.Nombre;
                    sqlComando.CommandType = CommandType.StoredProcedure;
                    if (!(_StoreProcedure.Parametros.Count == 0))
                    {
                        foreach (KeyValuePair<string, object> oP in _StoreProcedure.Parametros)
                        {
                            sqlComando.Parameters.AddWithValue(oP.Key, (oP.Value == null ? DBNull.Value : oP.Value));
                        }
                        _StoreProcedure.Parametros.Clear();
                    }
                    _StoreProcedure.Nombre = null;
                }
                return sqlComando;
            }
            catch (SqlException ExceptionErr)
            {
                //Deshace la transacción activa en caso de error
                if (blnUsarTransaccion)
                    sqlTransaccion.Rollback();
                throw new System.Exception(ExceptionErr.Number + "-" + ExceptionErr.Message, ExceptionErr.InnerException);
            }
        }

        /// <summary>
        /// Ejecuta un comando de acceso a la base de datos.
        /// </summary>
        /// <param name="tipoComando">Indica el tipo de comando que se va a ejecutar</param>
        /// <returns>Devuelve el resultado de la ejecución del comando</returns>
        /// <remarks></remarks>
        public object EjecutarCommand(ENUM_EXECUTECOMMAND tipoComando)
        {
            object functionReturnValue = null;
            SqlCommand sqlComando = null;

            try
            {
                //Configura el comando
                sqlComando = inicializarCommand();

                //Ejecuta el comando de acuerdo al tipo que se indica en el parámetro
                if (tipoComando == ENUM_EXECUTECOMMAND.NONQUERY)
                {
                    functionReturnValue = sqlComando.ExecuteNonQuery();
                }
                else if (tipoComando == ENUM_EXECUTECOMMAND.SCALAR)
                {
                    functionReturnValue = sqlComando.ExecuteScalar();
                }
                else
                {
                    functionReturnValue = sqlComando.ExecuteReader();
                }

            }
            catch (SqlException ExceptionErr)
            {
                //Deshace la transacción activa en caso de error
                if (blnUsarTransaccion)
                    rollBackTransaction();
                throw new System.Exception(ExceptionErr.Number + "-" + ExceptionErr.Message, ExceptionErr.InnerException);
            }
            finally
            {
                sqlComando.Dispose();
                sqlComando = null;
            }
            return functionReturnValue;

        }

        /// <summary>
        /// Carga el esquema de datos en el DataTable recibido como parámetro, 
        /// de acuerdo a la consulta cargada como propiedad, procedimiento 
        /// almacenado carcado como propiedad o a la consulta base de la tabla
        /// </summary>
        /// <param name="strTabla">Nombre de la tabla a partir de la que se creará el esquema</param>
        /// <returns>DataTable en el que se cargará el esquema</returns>
        /// <remarks></remarks>
        public DataTable CargarEsquema(string strTabla = "")
        {
            SqlDataAdapter objDataAdapter = null;
            SqlCommand sqlComando = null;
            DataTable objContenedor = new DataTable();
            try
            {
                //Para poder cargar el esquema en el contenedor debe haberse indicado la
                //tabla, consulta (propiedad) o procedimiento almacenado (propiedad)
                if (strTabla.Equals(string.Empty) & ((_Consulta == null || string.IsNullOrEmpty(_Consulta.Trim())) & (_StoreProcedure.Nombre == null)))
                {
                    throw new Exception("Información faltante para crear el esquema");
                }

                //En caso de que no se haya cargado la propiedad Consulta
                //se asigna la consulta base de la tabla
                if (!strTabla.Equals(string.Empty))
                {
                    _Consulta = "SELECT * FROM " + strTabla;
                }

                //Carga el esquema en el DataTable
                objDataAdapter = new SqlDataAdapter(_Consulta, sqlConexion);
                sqlComando = inicializarCommand();
                objDataAdapter.SelectCommand = sqlComando;
                objDataAdapter.FillSchema(objContenedor, SchemaType.Source);
                return objContenedor;

            }
            catch (SqlException OleDbExceptionErr)
            {
                //Deshace la transacción activa en caso de error
                if (blnUsarTransaccion)
                    rollBackTransaction();
                throw new System.Exception(OleDbExceptionErr.Message, OleDbExceptionErr.InnerException);
            }
            finally
            {
                objDataAdapter.Dispose();
                objDataAdapter = null;
            }
        }

        /// <summary>
        /// Levanta una actualización hacia la base de datos, a partir de los
        /// datos contenidos en un datatable o un dataset
        /// </summary>
        /// <param name="objContenedor">Objeto que contiene los datos que se van a actualizar
        /// en la base de datos</param>
        /// <remarks></remarks>
        public void ActualizarBD(object objContenedor)
        {
            SqlCommandBuilder sqlCommandBuilder = null;
            SqlCommand sqlComando = null;
            SqlDataAdapter sqlDataAdapter = null;
            string strSql = null;


            try
            {
                if (object.ReferenceEquals(objContenedor.GetType(), typeof(DataTable)))
                {
                    //Construye las operaciones de acceso a datos de acuerdo a la consulta
                    //cargada en la propiedad
                    sqlDataAdapter = new SqlDataAdapter(_Consulta, sqlConexion);
                    sqlComando = inicializarCommand();
                    sqlDataAdapter.SelectCommand = sqlComando;
                    sqlCommandBuilder = new SqlCommandBuilder(sqlDataAdapter);

                    //Utiliza las operaciones de acceso a datos configuradas anteriormente
                    //y levanta la actualización a partir de los datos en el data table.
                    DataTable dt = (DataTable)objContenedor;
                    sqlDataAdapter.Update(dt);
                    objContenedor = dt;

                }
                else if (object.ReferenceEquals(objContenedor.GetType(), typeof(DataSet)))
                {
                    //Construye las operaciones de acceso a datos de acuerdo a la consulta
                    //cargada en la propiedad y para cada una de las tablas.
                    //Levanta la actualización a partir de los datos de cada tabla del dataset
                    foreach (DataTable objDataTable in ((DataSet)objContenedor).Tables)
                    {
                        strSql = "SELECT * FROM " + objDataTable.TableName;
                        if (blnUsarTransaccion)
                            sqlDataAdapter = new SqlDataAdapter(strSql, sqlConexion);
                        sqlDataAdapter.SelectCommand.Transaction = sqlTransaccion;
                        sqlCommandBuilder = new SqlCommandBuilder(sqlDataAdapter);
                        DataSet ds = (DataSet)objContenedor;
                        sqlDataAdapter.Update(ds, objDataTable.TableName);
                        objContenedor = ds;
                    }

                }

            }
            catch (SqlException OleDbExceptionErr)
            {
                //Deshace la transacción activa en caso de error
                if (blnUsarTransaccion)
                    rollBackTransaction();
                throw new System.Exception(OleDbExceptionErr.Number + "-" + OleDbExceptionErr.Message, OleDbExceptionErr.InnerException);
            }
            finally
            {
                sqlDataAdapter.Dispose();
                sqlDataAdapter = null;
                sqlCommandBuilder.Dispose();
                sqlCommandBuilder = null;

            }
        }

        #region " Funciones publicas generales para ABC "

        // <summary>
        // Obtiene una fila de la tabla de la base de datos a la que corresponde el 
        // pojo enviado como parámetro
        // </summary>
        // <param name = "objPojo" > Objeto que representa una tabla de la base de datos,
        // sus atributos ayudan a armar la consulta SQL y a través de el se regresa
        // la información de la fila devuelta por la BD</param>
        // <remarks></remarks>
        public void TraeRegistro(BaseObject objPojo)
        {
            CampoValor cvAtributo = default(CampoValor);
            //Variable para cargar los datos de la fila a cada atributo del pojo
            string strConsulta = null;
            //Variable para almacenar la consulta para cargar la fila
            DataRow objFila = null;
            //Objeto para almacenar el registro que devuelva la BD
            DataSet objDataSet = null;
            //Objeto para almacenar el resultado de la consulta a la BD

            //Obtener el tipo y nombre de llave y nombre de la tabla para poder armar la consulta
            enumTipoDatoLlave = objPojo.TipoDatoLlave;
            strNombreTabla = objPojo.Tabla;
            cvLlaveTabla = objPojo.LlaveTabla;

            try
            {
                //Armar la consulta
                if (enumTipoDatoLlave == ENUM_TIPO_ID.NUMERO)
                {
                    strConsulta = "SELECT * FROM " + strNombreTabla + " WHERE " + cvLlaveTabla.Campo + "=" + cvLlaveTabla.Valor;
                }
                else
                {
                    strConsulta = "SELECT * FROM " + strNombreTabla + " WHERE " + cvLlaveTabla.Campo + " LIKE '" + cvLlaveTabla.Valor + "'";
                }

                //Realiza la consulta y llena el dataset con ayuda del método CargarDatos
                objDataSet = new DataSet();
                Consulta = strConsulta;
                objDataSet = CargarDatos(strNombreTabla);

                //Obtener la fila devuelta como resultado de la BD
                objFila = objDataSet.Tables[strNombreTabla].Rows[0];

                //Cargar los atributos del pojo con la información de la fila obtenida
                foreach (PropertyInfo infPropiedad in objPojo.GetType().GetProperties())
                {
                    if (object.ReferenceEquals(infPropiedad.PropertyType, typeof(CampoValor)))
                    {
                        cvAtributo = (CampoValor)infPropiedad.GetValue(objPojo, null);
                        cvAtributo.Valor = objFila[cvAtributo.Campo];
                        infPropiedad.SetValue(objPojo, cvAtributo, null);
                    }
                }

            }
            catch (Exception ExceptionErr)
            {
                if (blnUsarTransaccion)
                    rollBackTransaction();
                throw new System.Exception(ExceptionErr.Message, ExceptionErr.InnerException);
            }
            finally
            {
                objDataSet.Dispose();
                objDataSet = null;
            }
        }

        /// <summary>
        /// Método que llena un DataTable (con ayuda del método CargarDatosGenerico)
        /// de acuerdo a la consulta o stored procedure configurados en las 
        /// propiedades de la clase
        /// </summary>
        /// <returns>Tabla con los datos</returns>
        /// <remarks></remarks>
        public DataTable CargarDatos()
        {
            return (DataTable)CargarDatosGenerico();
        }

        /// <summary>
        /// Método que llena un DataTable (con ayuda del método CargarDatosGenerico)
        /// de acuerdo a la consulta o stored procedure configurados en las 
        /// propiedades de la clase
        /// </summary>
        /// <param name="tabla">Nombre de la tabla que se cargará</param>
        /// <returns>Conjunto de resultados</returns>
        /// <remarks></remarks>
        public DataSet CargarDatos(String tabla = "")
        {
            return (DataSet)CargarDatosGenerico(tabla);
        }

        /// <summary>
        /// Método que llena un DataSet o DataTable (dependiendo del tipo de objeto recibido
        /// en el parámetro objContenedor) de acuerdo a la consulta o stored procedure 
        /// configurados en las propiedades de la clase
        /// </summary>
        /// <param name="objContenedor">DataSet o DataTable en el que se devolverá el conjunto de 
        /// resultados obtenidos de la BD</param>
        /// <param name="strTabla">Nombre que se asignará a la tabla contenida en el Dataset</param>
        /// <remarks></remarks>
        private object CargarDatosGenerico(string strTabla = "")
        {
            //Objetos para ejecutar las sentencias en la base de datos
            SqlDataAdapter objDataAdapter = null;
            SqlCommand objCommand = null;
            object objContenedor = null;
            try
            {
                objDataAdapter = new SqlDataAdapter(_Consulta, sqlConexion);
                objCommand = inicializarCommand();
                objDataAdapter.SelectCommand = objCommand;

                //Llenar el contenedor con el conjunto de resultados devueltos de la base de datos
                if (!strTabla.Equals(String.Empty))
                {
                    DataSet ds = new DataSet();
                    objDataAdapter.Fill(ds, strTabla);
                    objContenedor = ds;
                }
                else
                {
                    DataTable dt = new DataTable();
                    objDataAdapter.Fill(dt);
                    objContenedor = dt;
                }
                return objContenedor;
            }
            catch (SqlException SqlExceptionErr)
            {
                if (blnUsarTransaccion)
                    rollBackTransaction();
                throw new System.Exception(SqlExceptionErr.Message, SqlExceptionErr.InnerException);
            }
            finally
            {
                objDataAdapter.Dispose();
                objDataAdapter = null;
                objCommand.Dispose();
                objCommand = null;
            }
        }

        /// <summary>
        /// Método que permite insertar un nuevo registro en la base de datos,
        /// con la información contenida en objPojo
        /// </summary>
        /// <param name="objPojo">Objeto que representa una tabla de la base de datos, 
        /// sus atributos ayudan a armar la consulta SQL y a cargar la información a insertar </param>
        /// <returns>Devuelve True si pudo realizar la inserción, de lo coantrario devuelve False</returns>
        /// <remarks></remarks>
        public bool AgregarRegistro(BaseObject objPojo)
        {
            bool blnCorrecto = false;
            //Variable que indicará si se pudo o no realizar la operación
            DataTable objDataTable = null;
            //Objeto en el que se cargará el esquema y los datos 
            //                                       para subir la actualización con el nuevo registro
            DataRow objFila = default(DataRow);
            //Objeto que permitirá cargar los datos del pojo en el datatable
            CampoValor cvAtributo = default(CampoValor);
            //Objeto que permitirá acceder a las propiedades del pojo

            try
            {
                //Obtener el nombre y la llave de la tabla y configurar la consulta
                strNombreTabla = objPojo.Tabla;
                cvLlaveTabla = objPojo.LlaveTabla;

                Consulta = "SELECT * FROM " + strNombreTabla;

                //Preparar el DataTable con el esquema de la tabla en la que se insertará
                objDataTable = CargarEsquema();

                //Agregar la nueva fila al datatable y llenarla con la información del pojo
                objFila = objDataTable.NewRow();


                foreach (PropertyInfo infPropiedad in objPojo.GetType().GetProperties())
                {
                    if (object.ReferenceEquals(infPropiedad.PropertyType, typeof(CampoValor)))
                    {
                        cvAtributo = (CampoValor)infPropiedad.GetValue(objPojo, null);

                        if (cvAtributo.Valor == null & cvAtributo.Campo == cvLlaveTabla.Campo)
                        {
                            cvAtributo.Valor = 0;
                        }
                        else if ((cvAtributo.Valor == null))
                        {
                            cvAtributo.Valor = DBNull.Value;
                        }
                        else if (object.ReferenceEquals(cvAtributo.Valor.GetType(), typeof(string)))
                        {
                            if (cvAtributo.Valor.ToString().Equals(string.Empty))
                                cvAtributo.Valor = DBNull.Value;
                        }

                        objFila[cvAtributo.Campo] = cvAtributo.Valor;
                    }

                }

                objDataTable.Rows.Add(objFila);

                //Subir la actualización del datatable a la base de datos
                ActualizarBD(objDataTable);

                blnCorrecto = true;

            }
            catch (SqlException ex)
            {
                blnCorrecto = false;
                if (blnUsarTransaccion)
                    rollBackTransaction();
                throw new System.Exception(ex.Number + "-" + ex.Message, ex.InnerException);
            }
            finally
            {
                objDataTable.Dispose();
                objDataTable = null;
            }

            return blnCorrecto;

        }

        /// <summary>
        /// Método que permite actualizar la información de un registro en la base de datos,
        /// con la información contenida en objPojo
        /// </summary>
        /// <param name="objPojo">Objeto que representa una tabla de la base de datos, 
        /// sus atributos ayudan a armar la consulta SQL y a cargar la información a actualizar </param>
        /// <returns>Devuelve True si pudo realizar la actualización, de lo coantrario devuelve False</returns>
        /// <remarks></remarks>
        public bool ModificarRegistro(BaseObject objPojo)
        {
            CampoValor cvAtributo = default(CampoValor);
            //Variable para cargar los datos del pojo a la fila
            bool blnCorrecto = false;
            //Variable que indicará si se pudo o no realizar la operación
            DataRow objFila = null;
            //Objeto para almacenar el registro que devuelva la BD
            DataSet objDataSet = null;
            //Objeto para almacenar el resultado de la consulta a la BD

            try
            {
                //Obtener el campo y tipo de la llave y el nombre de la tabla para configurar la consulta
                enumTipoDatoLlave = objPojo.TipoDatoLlave;
                strNombreTabla = objPojo.Tabla;
                cvLlaveTabla = objPojo.LlaveTabla;

                objDataSet = new DataSet();

                if (enumTipoDatoLlave == ENUM_TIPO_ID.NUMERO)
                {
                    Consulta = "SELECT * FROM " + strNombreTabla + " WHERE " + cvLlaveTabla.Campo + "=" + cvLlaveTabla.Valor;
                }
                else
                {
                    Consulta = "SELECT * FROM " + strNombreTabla + " WHERE " + cvLlaveTabla.Campo + " LIKE '" + cvLlaveTabla.Valor + "'";
                }


                //Carga la información actual de la base de datos para poder actualizarla
                objDataSet = CargarDatos(strNombreTabla);

                //Carga los nuevos valores en la fila
                objFila = objDataSet.Tables[strNombreTabla].Rows[0];
                objFila.BeginEdit();

                foreach (PropertyInfo infPropiedad in objPojo.GetType().GetProperties())
                {
                    if (object.ReferenceEquals(infPropiedad.PropertyType, typeof(CampoValor)))
                    {
                        cvAtributo = (CampoValor)infPropiedad.GetValue(objPojo, null);

                        if (cvAtributo.Valor == null & cvAtributo.Campo == cvLlaveTabla.Campo)
                        {
                            cvAtributo.Valor = 0;
                        }
                        else if ((cvAtributo.Valor == null))
                        {
                            cvAtributo.Valor = DBNull.Value;
                        }
                        else if (object.ReferenceEquals(cvAtributo.Valor.GetType(), typeof(string)))
                        {
                            if (cvAtributo.Valor.ToString().Equals(string.Empty))
                                cvAtributo.Valor = DBNull.Value;
                        }

                        objFila[cvAtributo.Campo] = cvAtributo.Valor;
                    }
                }

                objFila.EndEdit();

                //Sube la actualización de la fila a la BD
                ActualizarBD(objDataSet.Tables[strNombreTabla]);

                blnCorrecto = true;

            }
            catch (SqlException ex)
            {
                blnCorrecto = false;
                if (blnUsarTransaccion)
                    rollBackTransaction();
                throw new System.Exception(ex.Number + "-" + ex.Message, ex.InnerException);
            }
            finally
            {
                objDataSet.Dispose();
                objDataSet = null;
            }

            return blnCorrecto;

        }

        /// <summary>
        /// Método que permite eliminar un registro de la base de datos,
        /// con la información contenida en objPojo
        /// </summary>
        /// <param name="objPojo">Objeto que representa una tabla de la base de datos, 
        /// sus atributos ayudan a armar la sentencia SQL para eliminar</param>
        /// <returns>Devuelve True si pudo realizar la eliminación, de lo coantrario devuelve False</returns>
        /// <remarks></remarks>
        public bool EliminarRegistro(BaseObject objPojo)
        {
            bool blnCorrecto = false;
            //Variable que indicará si se pudo o no realizar la operación


            try
            {
                //Obtener el campo y tipo de la llave y el nombre de la tabla para configurar la sentencia SQL
                enumTipoDatoLlave = objPojo.TipoDatoLlave;
                cvLlaveTabla = objPojo.LlaveTabla;
                strNombreTabla = objPojo.Tabla;

                if (enumTipoDatoLlave == ENUM_TIPO_ID.NUMERO)
                {
                    Consulta = "DELETE FROM " + strNombreTabla + " WHERE " + cvLlaveTabla.Campo + "=" + cvLlaveTabla.Valor;
                }
                else
                {
                    Consulta = "DELETE FROM " + strNombreTabla + " WHERE " + cvLlaveTabla.Campo + " LIKE '" + cvLlaveTabla.Valor + "'";
                }

                //Elimina el registro de la base de datos
                EjecutarCommand(ENUM_EXECUTECOMMAND.NONQUERY);

                blnCorrecto = true;

            }
            catch (SqlException ex)
            {
                blnCorrecto = false;
                if (blnUsarTransaccion)
                    rollBackTransaction();
                throw new System.Exception(ex.Number + "-" + ex.Message, ex.InnerException);
            }

            return blnCorrecto;

        }

        #endregion

        #region " IDisposable Support "

        // Public implementation of Dispose pattern callable by consumers.
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        // Protected implementation of Dispose pattern.
        protected virtual void Dispose(bool disposing)
        {
            if (disposed)
                return;

            if (disposing)
            {
                if ((sqlConexion != null))
                {
                    if (sqlConexion.State == ConnectionState.Open)
                    {
                        if (blnUsarTransaccion)
                            sqlTransaccion.Commit();
                        sqlConexion.Close();
                    }
                    sqlConexion.Dispose();
                    sqlConexion = null;
                }

                if ((sqlTransaccion != null))
                {
                    sqlTransaccion.Dispose();
                    sqlTransaccion = null;
                }

                if ((_StoreProcedure != null))
                {
                    _StoreProcedure.Dispose();
                    _StoreProcedure = null;
                }

            }

            this.disposed = true;
        }

        ~AccesoDatosSICETemp()
        {
            Dispose(false);
        }
        #endregion

    }


    /// <summary>
    /// Clase que permite realizar la configuración de un stored procedure
    /// </summary>
    /// <remarks></remarks>
    public class InfoStoredProcedureSICETemp : IDisposable
    {
        //Variable para detectar llamadas redundantes
        bool disposed = false;

        private AccesoDatosSICETemp _ad;

        /// <summary>
        /// Constructor de la clase el cual inicializa los parámetros del procedimiento
        /// </summary>
        /// <remarks></remarks>
        public InfoStoredProcedureSICETemp(AccesoDatosSICETemp ad)
        {
            Parametros = new Dictionary<string, object>();
            _ad = ad;
        }

        /// <summary>
        /// Nombre del procedimiento almacenado
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public string Nombre { get; set; }

        /// <summary>
        /// Almacena el nombre y los valores de los parámetros del procedimiento almacenado.
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public Dictionary<string, object> Parametros { get; set; }

        /// <summary>
        /// Método que permite insertar un nuevo registro en la base de datos,
        /// con la información contenida en objPojo
        /// </summary>
        /// <param name="objPojo">Objeto que representa una tabla de la base de datos, 
        /// sus atributos ayudan a armar la consulta SQL y a cargar la información a insertar </param>
        /// <returns>Devuelve True si pudo realizar la inserción, de lo coantrario devuelve False</returns>
        /// <remarks></remarks>
        public bool AgregarRegistro(BaseObject objPojo)
        {
            bool blnCorrecto = false;
            //Variable que indicará si se pudo o no realizar la operación
            CampoValor cvAtributo = default(CampoValor);
            //Objeto que permitirá acceder a las propiedades del pojo

            _ad.StoreProcedure.Nombre = objPojo.Tabla;
            //se saca el nombre del procedimiento almacenado del pojo
            //si el valor de la llave primaria es diferente de nothing y de vacio significa que el valor no se
            if (objPojo.LlaveTabla.Valor != null)
            {
                //genera solo, se debe pasar el valor de la llave primaria por medio de un parametro al procedimiento almacenado
                if (object.ReferenceEquals(objPojo.LlaveTabla.Valor.GetType(), typeof(string)))
                {
                    if (!objPojo.LlaveTabla.Valor.ToString().Equals(string.Empty))
                    {
                        _ad.StoreProcedure.Parametros.Add("@" + objPojo.LlaveTabla.Campo, objPojo.LlaveTabla.Valor);
                        //se asigna la llave primaria junto con su valor como parametro al procedimiento almacenado
                    }
                }
                else
                {
                    _ad.StoreProcedure.Parametros.Add("@" + objPojo.LlaveTabla.Campo, objPojo.LlaveTabla.Valor);
                    //se asigna la llave primaria junto con su valor como parametro al procedimiento almacenado
                }
            }

            try
            {
                //se itera las propiedades del pojo que son del tipo CampoValor
                foreach (PropertyInfo infPropiedad in objPojo.GetType().GetProperties())
                {
                    if (object.ReferenceEquals(infPropiedad.PropertyType, typeof(CampoValor)))
                    {
                        cvAtributo = (CampoValor)infPropiedad.GetValue(objPojo, null);


                        if (cvAtributo.Campo != objPojo.LlaveTabla.Campo)
                        {
                            if ((cvAtributo.Valor == null))
                            {
                                cvAtributo.Valor = DBNull.Value;
                            }
                            else if (object.ReferenceEquals(cvAtributo.Valor.GetType(), typeof(string)))
                            {
                                if (cvAtributo.Valor.ToString().Equals(string.Empty))
                                    cvAtributo.Valor = DBNull.Value;
                            }

                            //Se agrega la propiedad al procedimiento almacenado como parametro
                            _ad.StoreProcedure.Parametros.Add("@" + cvAtributo.Campo, cvAtributo.Valor);

                        }

                    }

                }

                //Ejecutar el procedimiento almacenado
                _ad.EjecutarCommand(ENUM_EXECUTECOMMAND.NONQUERY);

                blnCorrecto = true;

            }
            catch (SqlException ex)
            {
                blnCorrecto = false;
                if (_ad.UsaTransaccion)
                    _ad.rollBackTransaction();
                throw new System.Exception(ex.Number + "-" + ex.Message, ex.InnerException);

            }
            finally
            {
            }

            return blnCorrecto;

        }


        /// <summary>
        /// Método que permite actualizar la información de un registro en la base de datos,
        /// con la información contenida en objPojo
        /// </summary>
        /// <param name="objPojo">Objeto que representa una tabla de la base de datos, 
        /// sus atributos ayudan a armar el procedimiento almacenado y a cargar la información a actualizar </param>
        /// <returns>Devuelve True si pudo realizar la actualización, de lo coantrario devuelve False</returns>
        /// <remarks></remarks>
        public bool ModificarRegistro(BaseObject objPojo)
        {
            CampoValor cvAtributo = default(CampoValor);
            //Variable para cargar los datos del pojo a la fila
            bool blnCorrecto = false;
            //Variable que indicará si se pudo o no realizar la operación

            _ad.StoreProcedure.Nombre = objPojo.Tabla;
            //se saca el nombre del procedimiento almacenado del pojo
            _ad.StoreProcedure.Parametros.Add("@" + objPojo.LlaveTabla.Campo, objPojo.LlaveTabla.Valor);
            //se asigna la llave primaria junto con su valor como parametro al procedimiento almacenado


            try
            {
                //se itera las propiedades del pojo que son del tipo CampoValor
                foreach (PropertyInfo infPropiedad in objPojo.GetType().GetProperties())
                {
                    //se verifica si la propiedad es de tipo CampoValor
                    if (object.ReferenceEquals(infPropiedad.PropertyType, typeof(CampoValor)))
                    {
                        cvAtributo = (CampoValor)infPropiedad.GetValue(objPojo, null);
                        //se obtiene la propiedad iterada

                        //se verifica si la propiedad que se esta iterando es la llave
                        if (cvAtributo.Campo != objPojo.LlaveTabla.Campo)
                        {
                            //sino esta debe agregarse al procedimeinto almacenado como un parametro

                            //si el campo valor de la propiedad es nothing se asigna DBNull.Value al valor de la propiedad
                            if ((cvAtributo.Valor == null))
                            {
                                cvAtributo.Valor = DBNull.Value;
                                //si el tipo de la propiedad es String y en el valor del campo tiene
                            }
                            else if (object.ReferenceEquals(cvAtributo.Valor.GetType(), typeof(string)))
                            {
                                //vacio String.Empty se asigna DBNull.Value al valor de la propiedad
                                if (cvAtributo.Valor.ToString().Equals(string.Empty))
                                    cvAtributo.Valor = DBNull.Value;
                            }

                            //Se agrega la propiedad al procedimiento almacenado como parametro
                            _ad.StoreProcedure.Parametros.Add("@" + cvAtributo.Campo, cvAtributo.Valor);

                        }


                    }
                }

                _ad.EjecutarCommand(ENUM_EXECUTECOMMAND.NONQUERY);

                blnCorrecto = true;

            }
            catch (SqlException ex)
            {
                blnCorrecto = false;
                if (_ad.UsaTransaccion)
                    _ad.rollBackTransaction();
                throw new System.Exception(ex.Number + "-" + ex.Message, ex.InnerException);

            }
            finally
            {
            }

            return blnCorrecto;

        }

        /// <summary>
        /// Obtiene una fila de la tabla de la base de datos a la que corresponde el 
        /// pojo enviado como parámetro
        /// </summary>
        /// <param name="objPojo">Objeto que representa una tabla de la base de datos, 
        /// sus atributos ayudan a armar la consulta SQL y a través de el se regresa
        /// la información de la fila devuelta por la BD </param>
        /// <remarks></remarks>
        public void TraeRegistro(BaseObject objPojo)
        {
            DataRow objFila = null;
            //Objeto para almacenar el registro que devuelva la BD
            CampoValor cvAtributo = default(CampoValor);
            //Variable para cargar los datos de la fila a cada atributo del pojo
            DataTable dt = new DataTable();
            //Almacena el registro que trae el procedimiento almacenado

            _ad.StoreProcedure.Nombre = objPojo.Tabla;
            //se saca el nombre del procedimiento almacenado del pojo
            _ad.StoreProcedure.Parametros.Add("@" + objPojo.LlaveTabla.Campo, objPojo.LlaveTabla.Valor);
            //se asigna la llave primaria junto con su valor como parametro al procedimiento almacenado

            dt = _ad.CargarDatos();
            //Obtener la fila devuelta como resultado de la BD
            objFila = dt.Rows[0];

            try
            {
                //Cargar los atributos del pojo con la información de la fila obtenida
                foreach (PropertyInfo infPropiedad in objPojo.GetType().GetProperties())
                {
                    if (object.ReferenceEquals(infPropiedad.PropertyType, typeof(CampoValor)))
                    {
                        cvAtributo = (CampoValor)infPropiedad.GetValue(objPojo, null);
                        cvAtributo.Valor = objFila[cvAtributo.Campo];
                        infPropiedad.SetValue(objPojo, cvAtributo, null);
                    }
                }

            }
            catch (Exception ExceptionErr)
            {
                if (_ad.UsaTransaccion)
                    _ad.rollBackTransaction();
                throw new System.Exception(ExceptionErr.Message, ExceptionErr.InnerException);
            }
            finally
            {
                dt.Dispose();
                dt = null;
            }
        }

        /// <summary>
        /// Método que permite eliminar un registro de la base de datos,
        /// con la información contenida en objPojo
        /// </summary>
        /// <param name="objPojo">Objeto que representa una tabla de la base de datos, 
        /// sus atributos ayudan a armar la sentencia SQL para eliminar</param>
        /// <returns>Devuelve True si pudo realizar la eliminación, de lo coantrario devuelve False</returns>
        /// <remarks></remarks>
        public bool EliminarRegistro(BaseObject objPojo)
        {
            bool blnCorrecto = false;
            //Variable que indicará si se pudo o no realizar la operación

            _ad.StoreProcedure.Nombre = objPojo.Tabla;
            //se saca el nombre del procedimiento almacenado del pojo
            _ad.StoreProcedure.Parametros.Add("@" + objPojo.LlaveTabla.Campo, objPojo.LlaveTabla.Valor);
            //se asigna la llave primaria junto con su valor como parametro al procedimiento almacenado


            try
            {
                //Se ejecuta el procedimiento almacenado
                _ad.EjecutarCommand(ENUM_EXECUTECOMMAND.NONQUERY);

                blnCorrecto = true;

            }
            catch (SqlException ex)
            {
                blnCorrecto = false;
                if (_ad.UsaTransaccion)
                    _ad.rollBackTransaction();
                throw new System.Exception(ex.Number + "-" + ex.Message, ex.InnerException);
            }

            return blnCorrecto;

        }

        #region " IDisposable Support "

        // Public implementation of Dispose pattern callable by consumers.
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        // Protected implementation of Dispose pattern.
        protected virtual void Dispose(bool disposing)
        {
            if (disposed)
                return;

            if (disposing)
            {
                Parametros = null;
                Nombre = null;

            }

            this.disposed = true;
        }

        ~InfoStoredProcedureSICETemp()
        {
            Dispose(false);
        }
        #endregion
    }

}