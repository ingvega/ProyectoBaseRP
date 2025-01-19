using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DATOS.UTIL
{
    public enum ENUM_TIPO_ID
    {
        /// <summary>
        /// Indica que el tipo de dato del ID de la tabla es numérico
        /// </summary>
        /// <remarks></remarks>
        NUMERO,

        /// <summary>
        /// Indica que el tipo de dato del ID de la tabla es alfanumérico
        /// </summary>
        /// <remarks></remarks>
        CADENA
    }

    /// <summary>
    /// Clase que representa el tipo de datos de los atributos que contienen los POJOs.
    /// </summary>
    /// <remarks></remarks>
    public class CampoValor
    {
        /// <summary>
        /// Almacena el valor del atributo
        /// </summary>
        /// <remarks></remarks>

        public System.Object Valor;
        /// <summary>
        /// Almacena el nombre del campo en la base de datos, para el atributo
        /// </summary>
        /// <remarks></remarks>
        public System.String Campo;
    }

    /// <summary>
    /// Clase base para todos los pojos
    /// </summary>
    /// <remarks></remarks>
    public abstract class BaseObject
    {
        //Variables para las propiedades
        protected string _Tabla;
        protected CampoValor _LlaveTabla = new CampoValor();

        protected ENUM_TIPO_ID _TipoDatoLlave;
        /// <summary>
        /// Almacena el nombre de la tabla a la cual representa el pojo (tal cual está en la BD)
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public string Tabla
        {
            get { return _Tabla; }
        }

        /// <summary>
        /// Almacena el valor y nombre (tal cual se incica en la BD) del campo llave de la tabla que representa el pojo
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public CampoValor LlaveTabla
        {
            get { return _LlaveTabla; }
            set { _LlaveTabla = value; }
        }

        /// <summary>
        /// Almacena el tipo de dato (cadena o número) del campo llave de la tabla que representa el pojo
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public ENUM_TIPO_ID TipoDatoLlave
        {
            get { return _TipoDatoLlave; }
        }


        public BaseObject()
        {
        }
    }

    /// <summary>
    /// 
    /// </summary>
    /// <remarks></remarks>
    public class ValidarDBNull
    {
        public static object ValidaDBNull(object oValor)
        {
            object oResult = null;

            if (System.Convert.IsDBNull(oValor))
            {
                return oResult;
            }
            else
            {
                oResult = oValor;
                return oResult;
            }

        }

    }

    /// <summary>
    /// Enumerado que se utiliza para identificar el mensaje de correo electronico que se debe
    /// enviar.
    /// </summary>
    /// <remarks></remarks>
    public enum ENUM_NOTIFICACIONES_EMAIL
    {
        VALIDAR_SOLICITUD_CORRECTA = 1,
        VALIDAR_SOLICITUD_INCORRECTA = 2,
        VALIDAR_DEPOSITO_CORRECTA = 3,
        VALIDAR_DEPOSITO_INCORRECTA = 4

    }

    /// <summary>
    /// Enumerado que se utiliza para identificar el mensaje de correo electronico que se debe
    /// enviar.
    /// </summary>
    /// <remarks></remarks>
    public enum ENUM_NOTIFICACIONES_EMAIL_TAGS
    {
        ALUMNO,
        FECHA_ACTUAL,
        NOTAS,
        PASSWORD_ALUMNO
    }
}
