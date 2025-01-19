using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DATOS.UTIL
{
    public enum ENUM_EXECUTECOMMAND
    {
        //''' <summary>
        //''' Indica que el comando una  vez que se ejecute, no devolverá ningún valor como respuesta
        //''' </summary>
        //''' <remarks></remarks>
        NONQUERY,

        //''' <summary>
        //''' Indica que el comando una  vez que se ejecute, devolverá un valor escalar como respuesta
        //''' </summary>
        //''' <remarks></remarks>
        SCALAR,

        //''' <summary>
        //''' Indica que el comando una  vez que se ejecute, devolverá un conjunto de filas como respuesta
        //''' </summary>
        //''' <remarks></remarks>
        READER
    }
}
