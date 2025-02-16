ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100    1) using System.ComponentModel;
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200    2) using System.Data;
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200    3) using System.Data.Common;
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100    4) using System.Globalization;
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200    5) using System.Linq.Expressions;
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200    6) using System.Reflection;
39204e38e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-11 16:52:40 +0100    7) using RepoDb.Enumerations;
39204e38e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-11 16:52:40 +0100    8) using RepoDb.Exceptions;
39204e38e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-11 16:52:40 +0100    9) using RepoDb.Extensions;
39204e38e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-11 16:52:40 +0100   10) using RepoDb.Interfaces;
39204e38e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-11 16:52:40 +0100   11) using RepoDb.Resolvers;
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200   12) 
d629d8cce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-12-18 13:42:51 +0100   13) namespace RepoDb.Reflection;
d629d8cce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-12-18 13:42:51 +0100   14) 
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200   15) /// <summary>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200   16) /// The compiler class of the library.
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200   17) /// </summary>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200   18) internal partial class Compiler
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200   19) {
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200   20)     #region SubClasses/SubStructs
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200   21) 
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200   22) 
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200   23)     /// <summary>
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200   24)     /// A class that contains both the instance of <see cref="RepoDb.ClassProperty"/> and <see cref="System.Reflection.ParameterInfo"/> objects.
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200   25)     /// </summary>
e906181ae RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 15:32:46 +0200   26)     internal class ClassPropertyParameterInfo
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200   27)     {
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200   28)         private string descriptiveContextString = null;
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200   29) 
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200   30)         /// <summary>
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200   31)         /// Gets the instance of <see cref="RepoDb.ClassProperty"/> object in used.
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200   32)         /// </summary>
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200   33)         public ClassProperty ClassProperty { get; set; }
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200   34) 
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200   35)         /// <summary>
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200   36)         /// Gets the instance of <see cref="System.Reflection.ParameterInfo"/> object in used.
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200   37)         /// </summary>
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200   38)         public ParameterInfo ParameterInfo { get; set; }
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200   39) 
d7b6efb91 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-12-04 16:27:24 +0100   40)         /// <summary>
d7b6efb91 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-12-04 16:27:24 +0100   41)         /// Gets the instance of <see cref="RepoDb.ClassProperty"/> object that is mapped to the current <see cref="ParameterInfo"/>.
d7b6efb91 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-12-04 16:27:24 +0100   42)         /// </summary>
d7b6efb91 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-12-04 16:27:24 +0100   43)         public ClassProperty ParameterInfoMappedClassProperty { get; set; }
d7b6efb91 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-12-04 16:27:24 +0100   44) 
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200   45)         /// <summary>
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200   46)         /// Gets the target type.
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200   47)         /// </summary>
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200   48)         public Type TargetType { get; set; }
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200   49) 
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200   50)         /// <summary>
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200   51)         /// Gets the target type based on the combinations.
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200   52)         /// </summary>
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200   53)         /// <returns></returns>
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200   54)         public Type GetTargetType() =>
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200   55)             TargetType ?? ParameterInfo?.ParameterType ?? ClassProperty?.PropertyInfo?.PropertyType;
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200   56) 
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200   57)         /// <summary>
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200   58)         /// Gets the descriptive context string for error messaging.
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200   59)         /// </summary>
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200   60)         /// <returns></returns>
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200   61)         internal string GetDescriptiveContextString()
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200   62)         {
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200   63)             if (descriptiveContextString != null)
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200   64)             {
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200   65)                 return descriptiveContextString;
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200   66)             }
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200   67) 
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200   68)             // Variable
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200   69)             var message = $"Context :: TargetType: {GetTargetType()} ";
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200   70) 
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200   71)             // ParameterInfo
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200   72)             if (ParameterInfo != null)
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200   73)             {
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200   74)                 message = string.Concat(descriptiveContextString, $"Parameter: {ParameterInfo.Name} ({ParameterInfo.ParameterType}) ");
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200   75)             }
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200   76) 
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200   77)             // ClassProperty
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200   78)             if (ClassProperty?.PropertyInfo != null)
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200   79)             {
153d0fcc7 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (fredliex              2021-01-17 16:18:29 +0800   80)                 message = string.Concat(descriptiveContextString, $"PropertyInfo: {ClassProperty.PropertyInfo.Name} ({ClassProperty.PropertyInfo.PropertyType}), DeclaringType: {ClassProperty.GetDeclaringType()} ");
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200   81)             }
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200   82) 
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200   83)             // Return
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200   84)             return (descriptiveContextString = message.Trim());
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200   85)         }
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200   86) 
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200   87)         /// <summary>
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200   88)         /// Returns the string that represents this object.
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200   89)         /// </summary>
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200   90)         /// <returns>The presented string.</returns>
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200   91)         public override string ToString() =>
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200   92)             string.Concat("TargetType = ", GetTargetType()?.FullName, ", ClassProperty = ", ClassProperty?.ToString(), ", ",
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200   93)                 "ParameterInfo = ", ParameterInfo?.ToString(), ")", ", TargetType = ", TargetType?.ToString(), ", ");
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200   94)     }
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200   95) 
e906181ae RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 15:32:46 +0200   96)     /// <summary>
e906181ae RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 15:32:46 +0200   97)     ///
e906181ae RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 15:32:46 +0200   98)     /// </summary>
e906181ae RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 15:32:46 +0200   99)     internal struct FieldDirection
e906181ae RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 15:32:46 +0200  100)     {
e906181ae RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 15:32:46 +0200  101)         public int Index { get; set; }
e906181ae RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 15:32:46 +0200  102)         public DbField DbField { get; set; }
e906181ae RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 15:32:46 +0200  103)         public ParameterDirection Direction { get; set; }
e906181ae RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 15:32:46 +0200  104)     }
e906181ae RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 15:32:46 +0200  105) 
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  106)     /// <summary>
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  107)     /// A class that contains both the property <see cref="MemberAssignment"/> object and the constructor argument <see cref="Expression"/> value.
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  108)     /// </summary>
d1c088f76 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-02-06 15:07:37 +0100  109)     internal class MemberBinding
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  110)     {
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  111)         /// <summary>
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  112)         /// Gets the instance of <see cref="ClassProperty"/> object in used.
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  113)         /// </summary>
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  114)         public ClassProperty ClassProperty { get; set; }
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  115) 
d1c088f76 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-02-06 15:07:37 +0100  116)         /// <summary>
d1c088f76 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-02-06 15:07:37 +0100  117)         /// Gets the instance of <see cref="ParameterInfo"/> object in used.
d1c088f76 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-02-06 15:07:37 +0100  118)         /// </summary>
d1c088f76 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-02-06 15:07:37 +0100  119)         public ParameterInfo ParameterInfo { get; set; }
d1c088f76 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-02-06 15:07:37 +0100  120) 
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  121)         /// <summary>
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  122)         /// Gets the current member assignment of the defined property.
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  123)         /// </summary>
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  124)         public MemberAssignment MemberAssignment { get; set; }
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  125) 
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  126)         /// <summary>
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  127)         /// Gets the corresponding constructor argument of the defined property.
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  128)         /// </summary>
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  129)         public Expression Argument { get; set; }
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  130) 
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  131)         /// <summary>
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  132)         /// Returns the string that represents this object.
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  133)         /// </summary>
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  134)         /// <returns>The presented string.</returns>
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  135)         public override string ToString() =>
d1c088f76 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-02-06 15:07:37 +0100  136)             ClassProperty?.ToString() ?? ParameterInfo?.ToString();
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  137)     }
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  138) 
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  139)     #endregion
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  140) 
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  141)     #region Methods
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  142) 
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200  143)     /// <summary>
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200  144)     ///
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200  145)     /// </summary>
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200  146)     /// <param name="fields"></param>
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200  147)     /// <returns></returns>
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200  148)     internal static IEnumerable<FieldDirection> GetInputFieldDirections(IEnumerable<DbField> fields)
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200  149)     {
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200  150)         if (fields?.Any() != true)
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200  151)         {
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200  152)             return Enumerable.Empty<FieldDirection>();
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200  153)         }
de7e83b76 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Simon Cropp           2021-01-17 17:25:37 +1100  154)         return fields.Select((value, index) => new FieldDirection
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200  155)         {
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200  156)             Index = index,
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200  157)             DbField = value,
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200  158)             Direction = ParameterDirection.Input
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200  159)         });
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200  160)     }
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200  161) 
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200  162)     /// <summary>
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200  163)     ///
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200  164)     /// </summary>
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200  165)     /// <param name="fields"></param>
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200  166)     /// <returns></returns>
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200  167)     internal static IEnumerable<FieldDirection> GetOutputFieldDirections(IEnumerable<DbField> fields)
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200  168)     {
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200  169)         if (fields?.Any() != true)
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200  170)         {
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200  171)             return Enumerable.Empty<FieldDirection>();
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200  172)         }
de7e83b76 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Simon Cropp           2021-01-17 17:25:37 +1100  173)         return fields.Select((value, index) => new FieldDirection
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200  174)         {
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200  175)             Index = index,
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200  176)             DbField = value,
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200  177)             Direction = ParameterDirection.Output
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200  178)         });
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200  179)     }
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200  180) 
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200  181)     /// <summary>
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200  182)     ///
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200  183)     /// </summary>
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200  184)     /// <param name="fromType"></param>
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200  185)     /// <param name="toType"></param>
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200  186)     /// <returns></returns>
46e99af30 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 16:10:18 +0200  187)     internal static MethodInfo GetSystemConvertToTypeMethod(Type fromType,
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200  188)         Type toType) =>
98fff6920 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (sergergood            2022-12-03 16:00:39 +0300  189)         StaticType.Convert.GetMethod(string.Concat("To", TypeCache.Get(toType).GetUnderlyingType().Name),
98fff6920 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (sergergood            2022-12-03 16:00:39 +0300  190)             new[] { TypeCache.Get(fromType).GetUnderlyingType() });
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200  191) 
46e99af30 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 16:10:18 +0200  192)     /// <summary>
46e99af30 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 16:10:18 +0200  193)     ///
46e99af30 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 16:10:18 +0200  194)     /// </summary>
46e99af30 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 16:10:18 +0200  195)     /// <param name="conversionType"></param>
46e99af30 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 16:10:18 +0200  196)     /// <returns></returns>
46e99af30 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 16:10:18 +0200  197)     internal static MethodInfo GetSystemConvertChangeTypeMethod(Type conversionType) =>
46e99af30 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 16:10:18 +0200  198)         StaticType.Convert.GetMethod("ChangeType",
98fff6920 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (sergergood            2022-12-03 16:00:39 +0300  199)             new[] { StaticType.Object, TypeCache.Get(conversionType).GetUnderlyingType() });
46e99af30 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 16:10:18 +0200  200) 
6f3a27f0e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 10:51:33 +0200  201)     /// <summary>
6f3a27f0e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 10:51:33 +0200  202)     ///
6f3a27f0e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 10:51:33 +0200  203)     /// </summary>
6f3a27f0e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 10:51:33 +0200  204)     /// <param name="type"></param>
6f3a27f0e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 10:51:33 +0200  205)     /// <returns></returns>
6f3a27f0e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 10:51:33 +0200  206)     internal static object GetClassHandler(Type type) =>
6f3a27f0e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 10:51:33 +0200  207)         ClassHandlerCache.Get<object>(type);
6f3a27f0e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 10:51:33 +0200  208) 
a1dba2363 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 15:21:58 +0200  209)     /// <summary>
a1dba2363 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 15:21:58 +0200  210)     ///
a1dba2363 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 15:21:58 +0200  211)     /// </summary>
a1dba2363 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 15:21:58 +0200  212)     /// <param name="handlerInstance"></param>
a1dba2363 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 15:21:58 +0200  213)     /// <returns></returns>
a1dba2363 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 15:21:58 +0200  214)     internal static MethodInfo GetClassHandlerGetMethod(object handlerInstance) =>
a1dba2363 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 15:21:58 +0200  215)         handlerInstance?.GetType().GetMethod("Get");
a1dba2363 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 15:21:58 +0200  216) 
6f3a27f0e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 10:51:33 +0200  217)     /// <summary>
6f3a27f0e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 10:51:33 +0200  218)     ///
6f3a27f0e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 10:51:33 +0200  219)     /// </summary>
6f3a27f0e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 10:51:33 +0200  220)     /// <param name="handlerInstance"></param>
6f3a27f0e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 10:51:33 +0200  221)     /// <returns></returns>
11bfcff6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-19 22:41:12 +0200  222)     internal static MethodInfo GetClassHandlerSetMethod(object handlerInstance) =>
11bfcff6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-19 22:41:12 +0200  223)         handlerInstance?.GetType().GetMethod("Set");
6f3a27f0e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 10:51:33 +0200  224) 
37916a409 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (XperiAndri            2022-09-18 23:32:19 +0300  225)     /// <summary>
37916a409 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (XperiAndri            2022-09-18 23:32:19 +0300  226)     ///
37916a409 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (XperiAndri            2022-09-18 23:32:19 +0300  227)     /// </summary>
37916a409 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (XperiAndri            2022-09-18 23:32:19 +0300  228)     /// <param name="handlerInstance"></param>
37916a409 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (XperiAndri            2022-09-18 23:32:19 +0300  229)     /// <returns></returns>
37916a409 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (XperiAndri            2022-09-18 23:32:19 +0300  230)     internal static Type GetPropertyHandlerInterfaceOrHandlerType(object handlerInstance)
37916a409 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (XperiAndri            2022-09-18 23:32:19 +0300  231)     {
37916a409 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (XperiAndri            2022-09-18 23:32:19 +0300  232)         if (handlerInstance is null) return null;
37916a409 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (XperiAndri            2022-09-18 23:32:19 +0300  233)         Type handlerType = handlerInstance.GetType();
37916a409 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (XperiAndri            2022-09-18 23:32:19 +0300  234)         var propertyHandlerInterface = handlerType
37916a409 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (XperiAndri            2022-09-18 23:32:19 +0300  235)             .GetInterfaces()
37916a409 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (XperiAndri            2022-09-18 23:32:19 +0300  236)             .FirstOrDefault(interfaceType =>
37916a409 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (XperiAndri            2022-09-18 23:32:19 +0300  237)                 interfaceType.IsInterfacedTo(StaticType.IPropertyHandler));
37916a409 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (XperiAndri            2022-09-18 23:32:19 +0300  238)         return propertyHandlerInterface ?? handlerType;
37916a409 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (XperiAndri            2022-09-18 23:32:19 +0300  239)     }
37916a409 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (XperiAndri            2022-09-18 23:32:19 +0300  240) 
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  241)     /// <summary>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  242)     ///
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  243)     /// </summary>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  244)     /// <param name="handlerInstance"></param>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  245)     /// <returns></returns>
6f3a27f0e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 10:51:33 +0200  246)     internal static MethodInfo GetPropertyHandlerGetMethod(object handlerInstance) =>
952ab4650 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-02-14 23:51:20 +0100  247)         // In F#, the instance is not a concrete class, therefore, we need to extract it by interface
37916a409 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (XperiAndri            2022-09-18 23:32:19 +0300  248)         GetPropertyHandlerInterfaceOrHandlerType(handlerInstance)?.GetMethod("Get");
952ab4650 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-02-14 23:51:20 +0100  249) 
952ab4650 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-02-14 23:51:20 +0100  250)     /// <summary>
952ab4650 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-02-14 23:51:20 +0100  251)     ///
952ab4650 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-02-14 23:51:20 +0100  252)     /// </summary>
952ab4650 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-02-14 23:51:20 +0100  253)     /// <param name="handlerInstance"></param>
952ab4650 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-02-14 23:51:20 +0100  254)     /// <returns></returns>
952ab4650 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-02-14 23:51:20 +0100  255)     internal static MethodInfo GetPropertyHandlerGetMethodFromInterface(object handlerInstance)
952ab4650 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-02-14 23:51:20 +0100  256)     {
952ab4650 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-02-14 23:51:20 +0100  257)         var propertyHandlerInterface = handlerInstance?
952ab4650 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-02-14 23:51:20 +0100  258)             .GetType()?
952ab4650 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-02-14 23:51:20 +0100  259)             .GetInterfaces()
952ab4650 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-02-14 23:51:20 +0100  260)             .FirstOrDefault(interfaceType =>
952ab4650 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-02-14 23:51:20 +0100  261)                 interfaceType.IsInterfacedTo(StaticType.IPropertyHandler));
952ab4650 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-02-14 23:51:20 +0100  262)         return propertyHandlerInterface?.GetMethod("Get");
952ab4650 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-02-14 23:51:20 +0100  263)     }
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  264) 
5aea813e3 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-19 14:50:52 +0200  265)     /// <summary>
5aea813e3 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-19 14:50:52 +0200  266)     ///
5aea813e3 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-19 14:50:52 +0200  267)     /// </summary>
5aea813e3 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-19 14:50:52 +0200  268)     /// <returns></returns>
5aea813e3 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-19 14:50:52 +0200  269)     internal static MethodInfo GetDbCommandCreateParameterMethod() =>
000000000 src/RepoDb/Reflection/Compiler/Compiler.cs         (Not Committed Yet     2025-02-16 11:02:49 +0100  270)         StaticType.DbCommandExtension.GetMethod(nameof(DbCommandExtension.CreateParameter), new[]
5aea813e3 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-19 14:50:52 +0200  271)         {
5aea813e3 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-19 14:50:52 +0200  272)             StaticType.IDbCommand,
5aea813e3 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-19 14:50:52 +0200  273)             StaticType.String,
5aea813e3 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-19 14:50:52 +0200  274)             StaticType.Object,
5aea813e3 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-19 14:50:52 +0200  275)             StaticType.DbTypeNullable
5aea813e3 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-19 14:50:52 +0200  276)         });
5aea813e3 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-19 14:50:52 +0200  277) 
5aea813e3 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-19 14:50:52 +0200  278)     /// <summary>
5aea813e3 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-19 14:50:52 +0200  279)     ///
5aea813e3 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-19 14:50:52 +0200  280)     /// </summary>
5aea813e3 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-19 14:50:52 +0200  281)     /// <returns></returns>
5aea813e3 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-19 14:50:52 +0200  282)     internal static MethodInfo GetDbParameterCollectionAddMethod() =>
5aea813e3 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-19 14:50:52 +0200  283)         StaticType.DbParameterCollection.GetMethod("Add");
5aea813e3 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-19 14:50:52 +0200  284) 
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200  285)     /// <summary>
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200  286)     ///
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200  287)     /// </summary>
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200  288)     /// <param name="handlerInstance"></param>
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200  289)     /// <returns></returns>
6f3a27f0e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 10:51:33 +0200  290)     internal static MethodInfo GetPropertyHandlerSetMethod(object handlerInstance) =>
37916a409 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (XperiAndri            2022-09-18 23:32:19 +0300  291)         GetPropertyHandlerInterfaceOrHandlerType(handlerInstance)?.GetMethod("Set");
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200  292) 
2a2d1318e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-19 21:45:47 +0200  293)     /// <summary>
2a2d1318e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-19 21:45:47 +0200  294)     ///
2a2d1318e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-19 21:45:47 +0200  295)     /// </summary>
2a2d1318e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-19 21:45:47 +0200  296)     /// <param name="property"></param>
2a2d1318e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-19 21:45:47 +0200  297)     /// <returns></returns>
2a2d1318e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-19 21:45:47 +0200  298)     internal static Type GetPropertyHandlerSetMethodReturnType(ClassProperty property) =>
2a2d1318e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-19 21:45:47 +0200  299)         GetPropertyHandlerSetMethod(property?.GetPropertyHandler())?.ReturnType;
2a2d1318e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-19 21:45:47 +0200  300) 
2a2d1318e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-19 21:45:47 +0200  301)     /// <summary>
2a2d1318e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-19 21:45:47 +0200  302)     ///
2a2d1318e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-19 21:45:47 +0200  303)     /// </summary>
2a2d1318e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-19 21:45:47 +0200  304)     /// <param name="handlerInstance"></param>
2a2d1318e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-19 21:45:47 +0200  305)     /// <returns></returns>
2a2d1318e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-19 21:45:47 +0200  306)     internal static Type GetPropertyHandlerSetMethodReturnType(object handlerInstance) =>
2a2d1318e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-19 21:45:47 +0200  307)         GetPropertyHandlerSetMethod(handlerInstance)?.ReturnType;
2a2d1318e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-19 21:45:47 +0200  308) 
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  309)     /// <summary>
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  310)     ///
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  311)     /// </summary>
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  312)     /// <param name="classPropertyParameterInfo"></param>
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  313)     /// <returns></returns>
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  314)     internal static ParameterInfo GetPropertyHandlerGetParameter(ClassPropertyParameterInfo classPropertyParameterInfo) =>
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200  315)         GetPropertyHandlerGetParameter(classPropertyParameterInfo?.ClassProperty);
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  316) 
2a2d1318e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-19 21:45:47 +0200  317)     /// <summary>
2a2d1318e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-19 21:45:47 +0200  318)     ///
2a2d1318e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-19 21:45:47 +0200  319)     /// </summary>
2a2d1318e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-19 21:45:47 +0200  320)     /// <param name="property"></param>
2a2d1318e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-19 21:45:47 +0200  321)     /// <param name="targetType"></param>
2a2d1318e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-19 21:45:47 +0200  322)     /// <returns></returns>
2a2d1318e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-19 21:45:47 +0200  323)     internal static Type GetPropertyHandlerSetMethodReturnType(ClassProperty property,
2a2d1318e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-19 21:45:47 +0200  324)         Type targetType) =>
2a2d1318e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-19 21:45:47 +0200  325)         GetPropertyHandlerSetMethod(property?.GetPropertyHandler() ??
2a2d1318e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-19 21:45:47 +0200  326)             PropertyHandlerCache.Get<object>(targetType))?.ReturnType;
2a2d1318e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-19 21:45:47 +0200  327) 
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  328)     /// <summary>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  329)     ///
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  330)     /// </summary>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  331)     /// <param name="classProperty"></param>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  332)     /// <returns></returns>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  333)     internal static ParameterInfo GetPropertyHandlerGetParameter(ClassProperty classProperty) =>
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  334)         GetPropertyHandlerGetParameter(classProperty?.GetPropertyHandler());
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  335) 
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  336)     /// <summary>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  337)     ///
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  338)     /// </summary>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  339)     /// <param name="handlerInstance"></param>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  340)     /// <returns></returns>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  341)     internal static ParameterInfo GetPropertyHandlerGetParameter(object handlerInstance) =>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  342)         GetPropertyHandlerGetParameter(GetPropertyHandlerGetMethod(handlerInstance));
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  343) 
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  344)     /// <summary>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  345)     ///
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  346)     /// </summary>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  347)     /// <param name="getMethod"></param>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  348)     /// <returns></returns>
6f3a27f0e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 10:51:33 +0200  349)     internal static ParameterInfo GetPropertyHandlerGetParameter(MethodInfo getMethod) =>
de7e83b76 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Simon Cropp           2021-01-17 17:25:37 +1100  350)         getMethod?.GetParameters().First();
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  351) 
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200  352)     /// <summary>
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200  353)     ///
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200  354)     /// </summary>
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200  355)     /// <param name="handlerInstance"></param>
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200  356)     /// <returns></returns>
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200  357)     internal static ParameterInfo GetPropertyHandlerSetParameter(object handlerInstance) =>
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200  358)         GetPropertyHandlerSetParameter(GetPropertyHandlerSetMethod(handlerInstance));
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200  359) 
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200  360)     /// <summary>
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200  361)     ///
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200  362)     /// </summary>
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200  363)     /// <param name="setMethod"></param>
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200  364)     /// <returns></returns>
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200  365)     internal static ParameterInfo GetPropertyHandlerSetParameter(MethodInfo setMethod) =>
de7e83b76 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Simon Cropp           2021-01-17 17:25:37 +1100  366)         setMethod?.GetParameters().First();
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200  367) 
e56040d0a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-05 15:09:59 +0200  368)     /// <summary>
e56040d0a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-05 15:09:59 +0200  369)     ///
e56040d0a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-05 15:09:59 +0200  370)     /// </summary>
e56040d0a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-05 15:09:59 +0200  371)     /// <param name="reader"></param>
e56040d0a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-05 15:09:59 +0200  372)     /// <param name="dbSetting"></param>
e56040d0a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-05 15:09:59 +0200  373)     /// <returns></returns>
e56040d0a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-05 15:09:59 +0200  374)     internal static IEnumerable<DataReaderField> GetDataReaderFields(DbDataReader reader,
e56040d0a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-05 15:09:59 +0200  375)         IDbSetting dbSetting) =>
e56040d0a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-05 15:09:59 +0200  376)         GetDataReaderFields(reader, null, dbSetting);
e56040d0a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-05 15:09:59 +0200  377) 
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  378)     /// <summary>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  379)     ///
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  380)     /// </summary>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  381)     /// <param name="reader"></param>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  382)     /// <param name="dbFields"></param>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  383)     /// <param name="dbSetting"></param>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  384)     /// <returns></returns>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  385)     internal static IEnumerable<DataReaderField> GetDataReaderFields(DbDataReader reader,
179ab9829 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (sergergood            2022-12-06 22:07:48 +0300  386)         DbFieldCollection dbFields,
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  387)         IDbSetting dbSetting)
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  388)     {
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  389)         return Enumerable.Range(0, reader.FieldCount)
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  390)             .Select(reader.GetName)
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  391)             .Select((name, ordinal) => new DataReaderField
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  392)             {
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  393)                 Name = name,
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  394)                 Ordinal = ordinal,
7d1c5279b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-16 16:53:16 +0200  395)                 Type = reader.GetFieldType(ordinal) ?? StaticType.Object,
179ab9829 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (sergergood            2022-12-06 22:07:48 +0300  396)                 DbField = dbFields?.GetByUnquotedName(name.AsUnquoted(true, dbSetting))
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  397)             });
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  398)     }
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  399) 
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  400)     /// <summary>
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  401)     ///
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  402)     /// </summary>
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  403)     /// <param name="classPropertyParameterInfo"></param>
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  404)     /// <param name="readerField"></param>
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  405)     /// <returns></returns>
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  406)     internal static object GetHandlerInstance(ClassPropertyParameterInfo classPropertyParameterInfo,
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  407)         DataReaderField readerField) =>
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  408)         GetHandlerInstance(classPropertyParameterInfo.ClassProperty, readerField);
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200  409) 
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  410)     /// <summary>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  411)     ///
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  412)     /// </summary>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  413)     /// <param name="classProperty"></param>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  414)     /// <param name="readerField"></param>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  415)     /// <returns></returns>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  416)     internal static object GetHandlerInstance(ClassProperty classProperty,
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  417)         DataReaderField readerField)
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  418)     {
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  419)         if (classProperty == null)
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  420)         {
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  421)             return null;
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  422)         }
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  423)         var value = classProperty.GetPropertyHandler();
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  424)         if (value == null && readerField?.Type != null)
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  425)         {
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  426)             value = PropertyHandlerCache
98fff6920 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (sergergood            2022-12-03 16:00:39 +0300  427)                 .Get<object>(TypeCache.Get(readerField.Type).GetUnderlyingType());
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  428)         }
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  429)         return value;
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  430)     }
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  431) 
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  432)     /// <summary>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  433)     ///
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  434)     /// </summary>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  435)     /// <param name="readerField"></param>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  436)     /// <returns></returns>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  437)     internal static MethodInfo GetDbReaderGetValueMethod(DataReaderField readerField) =>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  438)         GetDbReaderGetValueMethod(readerField.Type);
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  439) 
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  440)     /// <summary>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  441)     ///
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  442)     /// </summary>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  443)     /// <param name="targetType"></param>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  444)     /// <returns></returns>
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200  445)     internal static MethodInfo GetDbReaderGetValueMethod(Type targetType) =>
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200  446)         StaticType.DbDataReader.GetMethod(string.Concat("Get", targetType?.Name));
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  447) 
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  448)     /// <summary>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  449)     ///
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  450)     /// </summary>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  451)     /// <returns></returns>
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200  452)     internal static MethodInfo GetDbReaderGetValueMethod() =>
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200  453)         StaticType.DbDataReader.GetMethod("GetValue");
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  454) 
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  455)     /// <summary>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  456)     ///
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  457)     /// </summary>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  458)     /// <param name="readerField"></param>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  459)     /// <returns></returns>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  460)     internal static MethodInfo GetDbReaderGetValueOrDefaultMethod(DataReaderField readerField) =>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  461)         GetDbReaderGetValueOrDefaultMethod(readerField.Type);
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  462) 
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  463)     /// <summary>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  464)     ///
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  465)     /// </summary>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  466)     /// <param name="targetType"></param>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  467)     /// <returns></returns>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  468)     internal static MethodInfo GetDbReaderGetValueOrDefaultMethod(Type targetType) =>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  469)         GetDbReaderGetValueMethod(targetType) ?? GetDbReaderGetValueMethod();
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  470) 
46c096722 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 16:12:57 +0200  471)     /// <summary>
46c096722 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 16:12:57 +0200  472)     ///
46c096722 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 16:12:57 +0200  473)     /// </summary>
46c096722 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 16:12:57 +0200  474)     /// <returns></returns>
46c096722 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 16:12:57 +0200  475)     internal static MethodInfo GetDbParameterValueSetMethod() =>
46c096722 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 16:12:57 +0200  476)         StaticType.DbParameter.GetProperty("Value").SetMethod;
46c096722 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 16:12:57 +0200  477) 
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  478)     /// <summary>
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  479)     ///
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  480)     /// </summary>
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  481)     /// <returns></returns>
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  482)     internal static PropertyInfo GetTimeSpanTicksProperty() =>
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  483)         StaticType.TimeSpan.GetProperty("Ticks");
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  484) 
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  485)     /// <summary>
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  486)     ///
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  487)     /// </summary>
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  488)     /// <returns></returns>
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  489)     internal static MethodInfo GetTimeSpanTicksPropertyGetMethod() =>
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  490)         GetTimeSpanTicksProperty().GetMethod;
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  491) 
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  492)     /// <summary>
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  493)     ///
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  494)     /// </summary>
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  495)     /// <returns></returns>
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  496)     internal static PropertyInfo GetDateTimeTimeOfDayProperty() =>
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  497)         StaticType.DateTime.GetProperty("TimeOfDay");
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  498) 
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  499)     /// <summary>
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  500)     ///
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  501)     /// </summary>
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  502)     /// <returns></returns>
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  503)     internal static MethodInfo GetDateTimeTimeOfDayPropertyGetMethod() =>
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  504)         GetDateTimeTimeOfDayProperty().GetMethod;
a1467a9b8 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Andrii Chebukin       2023-09-13 23:52:10 +0400  505) #if NET6_0_OR_GREATER
4b02b04fd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Andrii Chebukin       2023-09-13 23:22:26 +0400  506)     /// <summary>
4b02b04fd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Andrii Chebukin       2023-09-13 23:22:26 +0400  507)     ///
4b02b04fd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Andrii Chebukin       2023-09-13 23:22:26 +0400  508)     /// </summary>
4b02b04fd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Andrii Chebukin       2023-09-13 23:22:26 +0400  509)     /// <returns></returns>
4b02b04fd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Andrii Chebukin       2023-09-13 23:22:26 +0400  510)     internal static MethodInfo GetDateOnlyFromDateTimeStaticMethod() =>
4b02b04fd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Andrii Chebukin       2023-09-13 23:22:26 +0400  511)         StaticType.DateOnly.GetMethod("FromDateTime");
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200  512) 
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200  513)     /// <summary>
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200  514)     ///
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200  515)     /// </summary>
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200  516)     /// <returns></returns>
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200  517)     internal static MethodInfo GetDateTimeFromDateOnlyMethod() =>
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200  518)         StaticType.DateOnly.GetMethod("ToDateTime", new Type[] { StaticType.TimeOnly });
a1467a9b8 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Andrii Chebukin       2023-09-13 23:52:10 +0400  519) #endif
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200  520) 
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200  521)     /// <summary>
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200  522)     ///
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200  523)     /// </summary>
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200  524)     /// <returns></returns>
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200  525)     internal static MethodInfo GetEnumGetNameMethod() =>
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200  526)         StaticType.Enum.GetMethod("GetName", new[] { StaticType.Type, StaticType.Object });
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200  527) 
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200  528)     /// <summary>
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200  529)     ///
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200  530)     /// </summary>
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200  531)     /// <returns></returns>
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200  532)     internal static MethodInfo GetEnumIsDefinedMethod() =>
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200  533)         StaticType.Enum.GetMethod("IsDefined", new[] { StaticType.Type, StaticType.Object });
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200  534) 
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  535) 
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  536)     internal static MethodInfo GetEnumParseNullMethod() =>
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  537)         typeof(Compiler).GetMethod(nameof(EnumParseNull), BindingFlags.Static | BindingFlags.NonPublic);
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  538) 
b94dfcf33 src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2025-02-03 14:10:54 +0100  539)     internal static MethodInfo GetEnumParseNullDefinedMethod() =>
b94dfcf33 src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2025-02-03 14:10:54 +0100  540)         typeof(Compiler).GetMethod(nameof(EnumParseNullDefined), BindingFlags.Static | BindingFlags.NonPublic);
b94dfcf33 src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2025-02-03 14:10:54 +0100  541) 
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  542)     private static TEnum? EnumParseNull<TEnum>(string value) where TEnum : struct, System.Enum
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  543)     {
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  544)         if (Enum.TryParse<TEnum>(value, true, out var r))
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  545)             return r;
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  546)         else
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  547)             return null;
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  548)     }
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  549) 
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  550)     private static TEnum? EnumParseNullDefined<TEnum>(string value) where TEnum : struct, System.Enum
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  551)     {
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  552)         if (Enum.TryParse<TEnum>(value, true, out var r) && Enum.IsDefined(typeof(TEnum), r))
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  553)             return r;
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  554)         else
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  555)             return null;
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  556)     }
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  557) 
df7555581 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-17 06:20:43 +0200  558)     /// <summary>
df7555581 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-17 06:20:43 +0200  559)     ///
df7555581 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-17 06:20:43 +0200  560)     /// </summary>
df7555581 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-17 06:20:43 +0200  561)     /// <param name="expression"></param>
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  562)     ///
df7555581 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-17 06:20:43 +0200  563)     /// <returns></returns>
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  564)     internal static Expression ConvertExpressionToNullableValue(Expression expression)
df7555581 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-17 06:20:43 +0200  565)     {
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  566)         if (Nullable.GetUnderlyingType(expression.Type) is { } underlyingType)
df7555581 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-17 06:20:43 +0200  567)         {
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  568)             return Expression.Property(expression, nameof(Nullable<int>.Value));
df7555581 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-17 06:20:43 +0200  569)         }
df7555581 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-17 06:20:43 +0200  570)         return expression;
df7555581 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-17 06:20:43 +0200  571)     }
df7555581 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-17 06:20:43 +0200  572) 
306e54f78 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 07:41:37 +0100  573) #if NET
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200  574)     internal static Expression ConvertExpressionToNullableGetValueOrDefaultExpression(Func<Expression, Expression> converter, Expression expression)
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200  575)     {
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200  576)         if (Nullable.GetUnderlyingType(expression.Type) != null)
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200  577)         {
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  578)             var converted = converter(ConvertExpressionToNullableValue(expression));
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200  579)             var nullableType = typeof(Nullable<>).MakeGenericType(converted.Type);
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200  580)             return Expression.Condition(
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200  581)                 Expression.Property(expression, nameof(Nullable<int>.HasValue)),
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200  582)                 Expression.Convert(converted, nullableType),
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200  583)                 Expression.Constant(null, nullableType)
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200  584)             );
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200  585)         }
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200  586) 
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200  587)         return converter(expression);
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200  588)     }
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200  589) #endif
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200  590) 
df7555581 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-17 06:20:43 +0200  591)     /// <summary>
df7555581 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-17 06:20:43 +0200  592)     ///
df7555581 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-17 06:20:43 +0200  593)     /// </summary>
df7555581 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-17 06:20:43 +0200  594)     /// <param name="expression"></param>
df7555581 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-17 06:20:43 +0200  595)     /// <returns></returns>
df7555581 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-17 06:20:43 +0200  596)     internal static Expression ConvertExpressionToNullableValueExpression(Expression expression)
df7555581 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-17 06:20:43 +0200  597)     {
df7555581 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-17 06:20:43 +0200  598)         if (Nullable.GetUnderlyingType(expression.Type) != null)
df7555581 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-17 06:20:43 +0200  599)         {
df7555581 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-17 06:20:43 +0200  600)             Expression.Call(expression, expression.Type.GetProperty("Value").GetMethod, expression);
df7555581 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-17 06:20:43 +0200  601)         }
df7555581 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-17 06:20:43 +0200  602)         return expression;
df7555581 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-17 06:20:43 +0200  603)     }
df7555581 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-17 06:20:43 +0200  604) 
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  605)     /// <summary>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  606)     ///
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  607)     /// </summary>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  608)     /// <param name="expression"></param>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  609)     /// <returns></returns>
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200  610)     internal static Expression ConvertExpressionToGuidToStringExpression(Expression expression) =>
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  611)         Expression.Call(ConvertExpressionToNullableValue(expression), StaticType.Guid.GetMethod("ToString", Array.Empty<Type>()));
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  612) 
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  613)     /// <summary>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  614)     ///
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  615)     /// </summary>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  616)     /// <param name="expression"></param>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  617)     /// <returns></returns>
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200  618)     internal static Expression ConvertExpressionToStringToGuidExpression(Expression expression) =>
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  619)         Expression.New(StaticType.Guid.GetConstructor(new[] { StaticType.String }), ConvertExpressionToNullableValue(expression));
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  620) 
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  621)     /// <summary>
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  622)     ///
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  623)     /// </summary>
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  624)     /// <param name="expression"></param>
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  625)     /// <returns></returns>
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  626)     internal static Expression ConvertExpressionToTimeSpanToDateTimeExpression(Expression expression) =>
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  627)         Expression.New(StaticType.DateTime.GetConstructor(new[] { StaticType.Int64 }),
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  628)             ConvertExpressionToNullableValue(ConvertExpressionToTimeSpanTicksExpression(expression)));
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  629) 
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  630)     /// <summary>
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  631)     ///
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  632)     /// </summary>
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  633)     /// <param name="expression"></param>
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  634)     /// <returns></returns>
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  635)     internal static Expression ConvertExpressionToDateTimeToTimeSpanExpression(Expression expression) =>
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  636)         ConvertExpressionToNullableValue(ConvertExpressionToDateTimeTimeOfDayExpression(expression));
306e54f78 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 07:41:37 +0100  637) #if NET
4b02b04fd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Andrii Chebukin       2023-09-13 23:22:26 +0400  638)     /// <summary>
4b02b04fd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Andrii Chebukin       2023-09-13 23:22:26 +0400  639)     ///
4b02b04fd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Andrii Chebukin       2023-09-13 23:22:26 +0400  640)     /// </summary>
4b02b04fd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Andrii Chebukin       2023-09-13 23:22:26 +0400  641)     /// <param name="expression"></param>
4b02b04fd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Andrii Chebukin       2023-09-13 23:22:26 +0400  642)     /// <returns></returns>
4b02b04fd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Andrii Chebukin       2023-09-13 23:22:26 +0400  643)     internal static Expression ConvertExpressionToDateTimeToDateOnlyExpression(Expression expression) =>
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200  644)         ConvertExpressionToNullableGetValueOrDefaultExpression(ConvertExpressionToDateTimeFromDateOnlyExpression, expression);
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200  645) 
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200  646)     /// <summary>
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200  647)     ///
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200  648)     /// </summary>
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200  649)     /// <param name="expression"></param>
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200  650)     /// <returns></returns>
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200  651)     internal static Expression ConvertExpressionToDateOnlyToDateTimeExpression(Expression expression) =>
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200  652)         ConvertExpressionToNullableGetValueOrDefaultExpression(ConvertExpressionToDateOnlyFromDateTimeExpression, expression);
a1467a9b8 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Andrii Chebukin       2023-09-13 23:52:10 +0400  653) #endif
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  654)     /// <summary>
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  655)     ///
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  656)     /// </summary>
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  657)     /// <param name="expression"></param>
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  658)     /// <returns></returns>
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  659)     internal static Expression ConvertExpressionToTimeSpanTicksExpression(Expression expression) =>
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  660)         Expression.Call(expression, GetTimeSpanTicksPropertyGetMethod());
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  661) 
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  662)     /// <summary>
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  663)     ///
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  664)     /// </summary>
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  665)     /// <param name="expression"></param>
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  666)     /// <returns></returns>
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  667)     internal static Expression ConvertExpressionToDateTimeTimeOfDayExpression(Expression expression) =>
b878f1b6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 11:59:43 +0200  668)         Expression.Call(expression, GetDateTimeTimeOfDayPropertyGetMethod());
306e54f78 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 07:41:37 +0100  669) #if NET
4b02b04fd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Andrii Chebukin       2023-09-13 23:22:26 +0400  670)     /// <summary>
4b02b04fd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Andrii Chebukin       2023-09-13 23:22:26 +0400  671)     ///
4b02b04fd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Andrii Chebukin       2023-09-13 23:22:26 +0400  672)     /// </summary>
4b02b04fd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Andrii Chebukin       2023-09-13 23:22:26 +0400  673)     /// <param name="expression"></param>
4b02b04fd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Andrii Chebukin       2023-09-13 23:22:26 +0400  674)     /// <returns></returns>
4b02b04fd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Andrii Chebukin       2023-09-13 23:22:26 +0400  675)     internal static Expression ConvertExpressionToDateOnlyFromDateTimeExpression(Expression expression) =>
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200  676)         Expression.Call(expression, GetDateTimeFromDateOnlyMethod(), Expression.Constant(default(TimeOnly)));
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200  677) 
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200  678)     /// <summary>
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200  679)     ///
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200  680)     /// </summary>
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200  681)     /// <param name="expression"></param>
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200  682)     /// <returns></returns>
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200  683)     internal static Expression ConvertExpressionToDateTimeFromDateOnlyExpression(Expression expression) =>
4b02b04fd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Andrii Chebukin       2023-09-13 23:22:26 +0400  684)         Expression.Call(null, GetDateOnlyFromDateTimeStaticMethod(), expression);
a1467a9b8 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Andrii Chebukin       2023-09-13 23:52:10 +0400  685) #endif
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  686)     /// <summary>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  687)     ///
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  688)     /// </summary>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  689)     /// <param name="expression"></param>
4a9b29eba RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 14:57:37 +0200  690)     /// <param name="toType"></param>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  691)     /// <returns></returns>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  692)     internal static Expression ConvertExpressionToSystemConvertExpression(Expression expression,
4a9b29eba RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 14:57:37 +0200  693)         Type toType)
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  694)     {
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  695)         var fromType = expression.Type;
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  696)         var underlyingFromType = TypeCache.Get(fromType).GetUnderlyingType();
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  697)         var underlyingToType = TypeCache.Get(toType).GetUnderlyingType();
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  698) 
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  699)         if (fromType == toType)
4a9b29eba RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 14:57:37 +0200  700)         {
4a9b29eba RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 14:57:37 +0200  701)             return expression;
4a9b29eba RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 14:57:37 +0200  702)         }
46e99af30 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 16:10:18 +0200  703) 
46e99af30 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 16:10:18 +0200  704)         // Identify
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  705)         if (underlyingToType.IsAssignableFrom(fromType))
3cffdb1bd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-15 23:06:39 +0200  706)         {
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  707)             return ConvertExpressionToTypeExpression(expression, underlyingToType);
46e99af30 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 16:10:18 +0200  708)         }
46e99af30 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 16:10:18 +0200  709) 
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  710)         var result = ConvertExpressionToNullableValue(expression);
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  711) 
46e99af30 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 16:10:18 +0200  712)         // Convert.To<Type>()
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  713)         if (underlyingFromType.IsEnum)
3cffdb1bd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-15 23:06:39 +0200  714)         {
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  715)             if (underlyingToType == StaticType.String)
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  716)             {
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  717)                 result = Expression.Call(result, nameof(ToString), Array.Empty<Type>());
3cffdb1bd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-15 23:06:39 +0200  718)             }
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  719)             else if (underlyingToType.IsPrimitive &&
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  720)                 (underlyingToType) == StaticType.Int16
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  721)                 || underlyingToType == StaticType.Int32
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  722)                 || underlyingToType == StaticType.Int64
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  723)                 || underlyingToType == StaticType.Byte
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  724)                 || underlyingToType == StaticType.UInt16
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  725)                 || underlyingToType == StaticType.UInt32
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  726)                 || underlyingToType == StaticType.UInt64
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  727)                 || underlyingToType == StaticType.SByte)
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  728)             {
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  729)                 result = Expression.Convert(result, Enum.GetUnderlyingType(underlyingFromType));
46e99af30 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 16:10:18 +0200  730) 
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  731)                 if (result.Type != underlyingToType)
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  732)                     result = Expression.Convert(result, underlyingToType);
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  733)             }
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  734)             else
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  735)                 return result; // Will fail
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  736)         }
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  737)         else if (toType == StaticType.String && fromType == StaticType.DateTime)
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  738)         {
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  739)             result = Expression.Call(GetStaticMethodInfo(() => StrictToString(DateTime.MinValue)), result);
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  740)         }
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  741)         else if (toType == StaticType.String && fromType == StaticType.DateTimeOffset)
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  742)         {
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  743)             result = Expression.Call(GetStaticMethodInfo(() => StrictToString(DateTimeOffset.MinValue)), result);
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  744)         }
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  745) #if NET
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  746)         else if (toType == StaticType.String && fromType == StaticType.DateOnly)
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  747)         {
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  748)             result = Expression.Call(GetStaticMethodInfo(() => StrictToString(DateOnly.MinValue)), result);
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  749)         }
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  750) #endif
15309a0cd src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2025-02-12 10:03:30 +0100  751)         else if (underlyingToType == StaticType.DateTime && underlyingFromType == StaticType.DateTimeOffset)
15309a0cd src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2025-02-12 10:03:30 +0100  752)         {
15309a0cd src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2025-02-12 10:03:30 +0100  753)             result = Expression.Property(result, nameof(DateTimeOffset.DateTime));
15309a0cd src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2025-02-12 10:03:30 +0100  754)         }
15309a0cd src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2025-02-12 10:03:30 +0100  755)         else if (underlyingToType == StaticType.DateTimeOffset && underlyingFromType == StaticType.DateTime)
15309a0cd src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2025-02-12 10:03:30 +0100  756)         {
84fe19e2b src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2025-02-12 14:25:47 +0100  757)             result = Expression.New(typeof(DateTimeOffset).GetConstructor([typeof(DateTime), typeof(TimeSpan)]), [result, Expression.Constant(TimeSpan.Zero)]);
15309a0cd src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2025-02-12 10:03:30 +0100  758)         }
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  759)         else if (GetSystemConvertToTypeMethod(underlyingFromType, underlyingToType) is { } methodInfo)
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  760)         {
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  761)             result = Expression.Call(methodInfo, Expression.Convert(result, methodInfo.GetParameters()[0].ParameterType));
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  762)         }
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  763)         else if (GetSystemConvertChangeTypeMethod(underlyingToType) is { } systemChangeType)
46e99af30 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 16:10:18 +0200  764)         {
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  765)             result = Expression.Call(systemChangeType, new Expression[]
46e99af30 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 16:10:18 +0200  766)             {
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  767)                 ConvertExpressionToTypeExpression(result, StaticType.Object),
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  768)                 Expression.Constant(TypeCache.Get(underlyingToType).GetUnderlyingType())
46e99af30 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 16:10:18 +0200  769)             });
3cffdb1bd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-15 23:06:39 +0200  770)         }
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  771)         else if (underlyingFromType == StaticType.String)
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  772)         {
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  773)             if (underlyingToType == StaticType.Decimal)
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  774)             {
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  775)                 result = Expression.Call(GetStaticMethodInfo(() => StrictParseDecimal(null)), expression);
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  776)             }
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  777)             else if (underlyingToType == StaticType.DateTime)
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  778)             {
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  779)                 result = Expression.Call(GetStaticMethodInfo(() => StrictParseDateTime(null)), expression);
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  780)             }
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  781)             else if (underlyingToType == StaticType.DateTimeOffset)
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  782)             {
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  783)                 result = Expression.Call(GetStaticMethodInfo(() => StrictParseDateTimeOffset(null)), expression);
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  784)             }
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  785) #if NET
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  786)             else if (underlyingToType == typeof(DateOnly))
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  787)             {
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  788)                 result = Expression.Call(GetStaticMethodInfo(() => StrictParseDateOnly(null)), expression);
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  789)             }
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  790) #endif
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  791)             else
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  792)                 return null;
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  793)         }
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  794)         else
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  795)         {
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  796)             return result; // Will fail!
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  797)         }
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  798) 
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  799)         // Do we need manual NULL handling?
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  800)         if ((!underlyingToType.IsValueType || underlyingToType != toType)
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  801)             && (!underlyingFromType.IsValueType || underlyingFromType != fromType))
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  802)         {
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  803)             Expression condition;
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  804)             if (underlyingFromType != fromType)
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  805)             {
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  806)                 // E.g. Nullable<System.Int32> -> string
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  807)                 condition = Expression.Property(expression, nameof(Nullable<int>.HasValue));
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  808)             }
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  809)             else
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  810)             {
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  811)                 // E.g. String -> Nullable<System.Int32>
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  812)                 condition = Expression.NotEqual(expression, Expression.Constant(null, expression.Type));
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  813)             }
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  814) 
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  815)             return Expression.Condition(
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  816)                 condition,
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  817)                 (result.Type != toType) ? Expression.Convert(result, toType) : result,
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  818)                 Expression.Constant(null, toType));
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  819)         }
46e99af30 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 16:10:18 +0200  820) 
46e99af30 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-25 16:10:18 +0200  821)         // Return
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  822)         return result;
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  823)     }
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  824) 
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  825)     static MethodInfo GetStaticMethodInfo(Expression<Action> call)
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  826)     {
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  827)         return (call.Body as MethodCallExpression)?.Method;
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  828)     }
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  829) 
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  830)     static decimal StrictParseDecimal(string value)
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  831)     {
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  832)         return decimal.Parse(value, CultureInfo.InvariantCulture);
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  833)     }
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  834) 
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  835)     static DateTime StrictParseDateTime(string value)
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  836)     {
170a53062 src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2025-02-14 19:19:55 +0100  837)         return DateTime.Parse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  838)     }
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  839) 
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  840)     static string StrictToString(DateTime value)
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  841)     {
24a6c59b8 src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2025-02-15 21:43:01 +0100  842)         return value.ToString("o", CultureInfo.InvariantCulture);
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  843)     }
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  844) 
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  845)     static string StrictToString(DateTimeOffset value)
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  846)     {
24a6c59b8 src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2025-02-15 21:43:01 +0100  847)         return value.ToString("o", CultureInfo.InvariantCulture);
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  848)     }
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  849) 
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  850)     static DateTimeOffset StrictParseDateTimeOffset(string value)
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  851)     {
24a6c59b8 src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2025-02-15 21:43:01 +0100  852)         return DateTimeOffset.Parse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.RoundtripKind); // If we have no offset assume no offset
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  853)     }
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  854) 
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  855) #if NET
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  856)     static DateOnly StrictParseDateOnly(string value)
15309a0cd src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2025-02-12 10:03:30 +0100  857)     {
15309a0cd src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2025-02-12 10:03:30 +0100  858)         try
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  859)         {
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  860)             return DateOnly.Parse(value, CultureInfo.InvariantCulture);
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  861)         }
15309a0cd src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2025-02-12 10:03:30 +0100  862)         catch (FormatException) when (DateTime.TryParse(value, CultureInfo.InvariantCulture, out var vv) && DateOnly.FromDateTime(vv) is { } dateOnly && dateOnly.ToDateTime(TimeOnly.MinValue) == vv)
15309a0cd src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2025-02-12 10:03:30 +0100  863)         {
15309a0cd src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2025-02-12 10:03:30 +0100  864)             return dateOnly; // We can't guarantee that DateOnly is only serialized in text form as dateonly :(
15309a0cd src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2025-02-12 10:03:30 +0100  865)         }
15309a0cd src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2025-02-12 10:03:30 +0100  866)     }
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  867) 
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  868)     static string StrictToString(DateOnly value)
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  869)     {
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  870)         return value.ToString("d", CultureInfo.InvariantCulture);
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  871)     }
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  872) #endif
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  873) 
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  874)     /// <summary>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  875)     ///
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  876)     /// </summary>
11df3563a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 14:15:02 +0200  877)     /// <param name="expression"></param>
11df3563a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 14:15:02 +0200  878)     /// <param name="toType"></param>
11df3563a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 14:15:02 +0200  879)     /// <returns></returns>
11df3563a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 14:15:02 +0200  880)     internal static Expression ConvertExpressionToTypeExpression(Expression expression,
11df3563a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 14:15:02 +0200  881)         Type toType) =>
11df3563a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 14:15:02 +0200  882)         (expression.Type != toType) ? Expression.Convert(expression, toType) : expression;
11df3563a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 14:15:02 +0200  883) 
e0d7ef75f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 15:39:29 +0200  884)     /// <summary>
e0d7ef75f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 15:39:29 +0200  885)     ///
e0d7ef75f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 15:39:29 +0200  886)     /// </summary>
e0d7ef75f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 15:39:29 +0200  887)     /// <param name="expression"></param>
e0d7ef75f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 15:39:29 +0200  888)     /// <param name="fromType"></param>
e0d7ef75f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 15:39:29 +0200  889)     /// <param name="toEnumType"></param>
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  890)     /// <param name="dbSetting"></param>
e0d7ef75f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 15:39:29 +0200  891)     /// <returns></returns>
e0d7ef75f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 15:39:29 +0200  892)     internal static Expression ConvertExpressionToEnumExpression(Expression expression,
e0d7ef75f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 15:39:29 +0200  893)         Type fromType,
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  894)         Type toEnumType,
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  895)         IDbSetting dbSetting) =>
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200  896)         (fromType == StaticType.String) ?
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  897)             ConvertExpressionToEnumExpressionForString(expression, toEnumType, dbSetting) :
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  898)                 ConvertExpressionToEnumExpressionForNonString(expression, toEnumType, dbSetting);
e0d7ef75f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 15:39:29 +0200  899) 
e0d7ef75f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 15:39:29 +0200  900)     /// <summary>
e0d7ef75f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 15:39:29 +0200  901)     ///
e0d7ef75f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 15:39:29 +0200  902)     /// </summary>
e0d7ef75f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 15:39:29 +0200  903)     /// <param name="expression"></param>
e0d7ef75f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 15:39:29 +0200  904)     /// <param name="toEnumType"></param>
e0d7ef75f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 15:39:29 +0200  905)     /// <returns></returns>
e0d7ef75f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 15:39:29 +0200  906)     internal static Expression ConvertExpressionToEnumExpressionForString(Expression expression,
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  907)         Type toEnumType,
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  908)         IDbSetting dbSetting)
e0d7ef75f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 15:39:29 +0200  909)     {
b94dfcf33 src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2025-02-03 14:10:54 +0100  910)         var func = ((GlobalConfiguration.Options.EnumHandling is InvalidEnumValueHandling.Cast || toEnumType.GetCustomAttribute<FlagsAttribute>() is { }) ? GetEnumParseNullMethod() : GetEnumParseNullDefinedMethod()).MakeGenericMethod(toEnumType);
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  911)         return Expression.Coalesce(
b94dfcf33 src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2025-02-03 14:10:54 +0100  912)                     Expression.Call(func, expression),
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  913) 
0aa7fd31d src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2024-12-19 13:46:43 +0100  914)                     (GlobalConfiguration.Options.EnumHandling == InvalidEnumValueHandling.UseDefault)
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  915)                     ? Expression.Default(toEnumType)
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  916)                     : Expression.Throw(Expression.New(
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  917)                         typeof(ArgumentOutOfRangeException).GetConstructor(new[] { StaticType.String, StaticType.Object, StaticType.String }),
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  918)                         Expression.Constant("value"),
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  919)                         expression,
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  920)                         Expression.Constant($"Invalid value for {toEnumType.Name}")),
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  921)                         toEnumType));
e0d7ef75f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 15:39:29 +0200  922)     }
e0d7ef75f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 15:39:29 +0200  923) 
e0d7ef75f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 15:39:29 +0200  924)     /// <summary>
e0d7ef75f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 15:39:29 +0200  925)     ///
e0d7ef75f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 15:39:29 +0200  926)     /// </summary>
e0d7ef75f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 15:39:29 +0200  927)     /// <param name="expression"></param>
e0d7ef75f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 15:39:29 +0200  928)     /// <param name="toEnumType"></param>
e0d7ef75f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 15:39:29 +0200  929)     /// <returns></returns>
e0d7ef75f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 15:39:29 +0200  930)     internal static Expression ConvertExpressionToEnumExpressionForNonString(Expression expression,
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  931)         Type toEnumType,
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100  932)         IDbSetting dbSetting)
e0d7ef75f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 15:39:29 +0200  933)     {
0aa7fd31d src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2024-12-19 13:46:43 +0100  934)         if (GlobalConfiguration.Options.EnumHandling == InvalidEnumValueHandling.Cast)
e0d7ef75f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 15:39:29 +0200  935)         {
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200  936)             return Expression.Convert(expression, toEnumType);
e0d7ef75f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 15:39:29 +0200  937)         }
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200  938)         else
e0d7ef75f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 15:39:29 +0200  939)         {
39204e38e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-11 16:52:40 +0100  940)             // Handle long/short to enum and/or non integer based enums
39204e38e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-11 16:52:40 +0100  941)             if (expression.Type != Enum.GetUnderlyingType(toEnumType))
39204e38e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-11 16:52:40 +0100  942)                 expression = Expression.Convert(expression, Enum.GetUnderlyingType(toEnumType));
39204e38e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-11 16:52:40 +0100  943) 
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  944)             return Expression.Condition(
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  945)                 GetEnumIsDefinedExpression(expression, toEnumType), // Check if the value is defined
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  946)                 Expression.Convert(expression, toEnumType), // Cast to enum
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  947)                 GlobalConfiguration.Options.EnumHandling switch
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  948)                 {
0aa7fd31d src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2024-12-19 13:46:43 +0100  949)                     InvalidEnumValueHandling.UseDefault => Expression.Default(toEnumType),
0aa7fd31d src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2024-12-19 13:46:43 +0100  950)                     InvalidEnumValueHandling.ThrowError => Expression.Throw(Expression.New(typeof(InvalidEnumArgumentException).GetConstructor(new[] { StaticType.String, StaticType.Int32, StaticType.Type }),
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  951)                                                                 new Expression[] { Expression.Constant("value"), Expression.Convert(expression, StaticType.Int32), Expression.Constant(toEnumType) }),
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  952)                         toEnumType
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  953)                     ),
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  954)                     _ => throw new InvalidEnumArgumentException("EnumHandling set to invalid value")
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100  955)                 }); // Default value for undefined
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200  956)         }
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  957)     }
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200  958) 
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200  959)     /// <summary>
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200  960)     ///
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200  961)     /// </summary>
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200  962)     /// <param name="expression"></param>
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200  963)     /// <param name="toType"></param>
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200  964)     /// <returns></returns>
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200  965)     internal static Expression ConvertEnumExpressionToTypeExpression(Expression expression,
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200  966)         Type toType)
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200  967)     {
98fff6920 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (sergergood            2022-12-03 16:00:39 +0300  968)         var underlyingType = TypeCache.Get(toType).GetUnderlyingType();
98fff6920 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (sergergood            2022-12-03 16:00:39 +0300  969)         if (underlyingType == StaticType.String || underlyingType == StaticType.Boolean)
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200  970)         {
ed0458e07 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-03 14:09:07 +0200  971)             return ConvertEnumExpressionToTypeExpressionForString(expression);
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200  972)         }
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200  973)         else
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200  974)         {
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200  975)             return ConvertEnumExpressionToTypeExpressionForNonString(expression, toType);
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200  976)         }
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200  977)     }
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200  978) 
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200  979)     /// <summary>
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200  980)     ///
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200  981)     /// </summary>
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200  982)     /// <param name="expression"></param>
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200  983)     /// <returns></returns>
ed0458e07 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-03 14:09:07 +0200  984)     internal static Expression ConvertEnumExpressionToTypeExpressionForString(Expression expression)
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200  985)     {
b42cbdf10 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-27 14:13:01 +0100  986)         var method = StaticType.Convert.GetMethod("ToString", new[] { StaticType.Object });
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200  987) 
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200  988)         // Variables
ed0458e07 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-03 14:09:07 +0200  989)         var isNullExpression = (Expression)null;
ed0458e07 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-03 14:09:07 +0200  990)         var trueExpression = (Expression)null;
ed0458e07 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-03 14:09:07 +0200  991)         var falseExpression = (Expression)null;
ed0458e07 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-03 14:09:07 +0200  992) 
ed0458e07 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-03 14:09:07 +0200  993)         // Ensure (Ref/Nullable)
5197d27f3 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (sergergood            2022-12-03 22:00:02 +0300  994)         if (TypeCache.Get(expression.Type).IsNullable())
ed0458e07 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-03 14:09:07 +0200  995)         {
ed0458e07 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-03 14:09:07 +0200  996)             // Check
ed0458e07 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-03 14:09:07 +0200  997)             isNullExpression = Expression.Equal(Expression.Constant(null), expression);
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200  998) 
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200  999)             // True
ed0458e07 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-03 14:09:07 +0200 1000)             trueExpression = Expression.Convert(Expression.Constant(null), StaticType.String);
ed0458e07 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-03 14:09:07 +0200 1001)         }
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200 1002) 
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200 1003)         // False
b42cbdf10 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-27 14:13:01 +0100 1004)         var methodCallExpression = Expression.Call(method, ConvertExpressionToTypeExpression(expression, StaticType.Object));
b42cbdf10 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-27 14:13:01 +0100 1005)         falseExpression = ConvertExpressionToTypeExpression(methodCallExpression, StaticType.String);
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200 1006) 
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200 1007)         // Call and return
ed0458e07 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-03 14:09:07 +0200 1008)         return isNullExpression == null ? falseExpression :
ed0458e07 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-03 14:09:07 +0200 1009)             Expression.Condition(isNullExpression, trueExpression, falseExpression);
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200 1010)     }
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200 1011) 
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200 1012)     /// <summary>
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200 1013)     ///
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200 1014)     /// </summary>
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200 1015)     /// <param name="expression"></param>
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200 1016)     /// <param name="toType"></param>
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200 1017)     /// <returns></returns>
ed0458e07 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-03 14:09:07 +0200 1018)     internal static Expression ConvertEnumExpressionToTypeExpressionForNonString(Expression expression,
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200 1019)         Type toType)
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200 1020)     {
ed0458e07 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-03 14:09:07 +0200 1021)         var isNullExpression = (Expression)null;
ed0458e07 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-03 14:09:07 +0200 1022)         var trueExpression = (Expression)null;
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200 1023)         var falseExpression = expression;
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200 1024) 
ed0458e07 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-03 14:09:07 +0200 1025)         // Ensure (Ref/Nullable)
5197d27f3 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (sergergood            2022-12-03 22:00:02 +0300 1026)         var cachedType = TypeCache.Get(expression.Type);
5197d27f3 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (sergergood            2022-12-03 22:00:02 +0300 1027)         if (cachedType.IsNullable())
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200 1028)         {
ed0458e07 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-03 14:09:07 +0200 1029)             isNullExpression = Expression.Equal(Expression.Constant(null), expression);
ed0458e07 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-03 14:09:07 +0200 1030)             trueExpression = GetNullableTypeExpression(toType);
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200 1031)         }
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200 1032) 
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200 1033)         // Casting
5197d27f3 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (sergergood            2022-12-03 22:00:02 +0300 1034)         if (cachedType.GetUnderlyingType() != TypeCache.Get(toType).GetUnderlyingType())
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200 1035)         {
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200 1036)             falseExpression = ConvertExpressionToTypeExpression(expression, toType);
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200 1037)         }
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200 1038) 
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200 1039)         // Nullable
5197d27f3 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (sergergood            2022-12-03 22:00:02 +0300 1040)         if (cachedType.IsNullable())
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200 1041)         {
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200 1042)             falseExpression = ConvertExpressionToNullableExpression(falseExpression, toType);
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200 1043)         }
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200 1044) 
f68ac7417 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:45:01 +0200 1045)         // Return
ed0458e07 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-03 14:09:07 +0200 1046)         return isNullExpression == null ? falseExpression :
ed0458e07 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-03 14:09:07 +0200 1047)             Expression.Condition(isNullExpression, trueExpression, falseExpression);
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1048)     }
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1049) 
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1050)     /// <summary>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1051)     ///
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1052)     /// </summary>
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1053)     /// <param name="expression"></param>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1054)     /// <returns></returns>
96293020f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-02-20 16:55:54 +0100 1055)     internal static Expression ConvertExpressionToDbNullExpression(Expression expression)
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1056)     {
87002d8ea RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-12 16:36:18 +0100 1057)         var valueIsNullExpression = Expression.Equal(expression, Expression.Constant(null));
87002d8ea RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-12 16:36:18 +0100 1058)         var dbNullValueExpresion = ConvertExpressionToTypeExpression(Expression.Constant(DBNull.Value), StaticType.Object);
87002d8ea RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-12 16:36:18 +0100 1059)         return Expression.Condition(valueIsNullExpression, dbNullValueExpresion, expression);
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1060)     }
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1061) 
e56040d0a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-05 15:09:59 +0200 1062)     /// <summary>
e56040d0a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-05 15:09:59 +0200 1063)     ///
e56040d0a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-05 15:09:59 +0200 1064)     /// </summary>
11df3563a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 14:15:02 +0200 1065)     /// <param name="expression"></param>
e56040d0a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-05 15:09:59 +0200 1066)     /// <param name="targetNullableType"></param>
e56040d0a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-05 15:09:59 +0200 1067)     /// <returns></returns>
11df3563a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 14:15:02 +0200 1068)     internal static Expression ConvertExpressionToNullableExpression(Expression expression,
e56040d0a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-05 15:09:59 +0200 1069)         Type targetNullableType)
e56040d0a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-05 15:09:59 +0200 1070)     {
2d0bbb3b6 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-08 00:49:48 +0100 1071)         if (expression.Type.IsValueType == false)
2d0bbb3b6 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-08 00:49:48 +0100 1072)         {
2d0bbb3b6 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-08 00:49:48 +0100 1073)             return expression;
2d0bbb3b6 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-08 00:49:48 +0100 1074)         }
2d0bbb3b6 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-08 00:49:48 +0100 1075) 
11df3563a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 14:15:02 +0200 1076)         var underlyingType = Nullable.GetUnderlyingType(expression.Type);
d4e90c09c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (sergergood            2022-12-08 21:20:16 +0300 1077)         targetNullableType = TypeCache.Get(targetNullableType).GetUnderlyingType();
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1078) 
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1079)         if (targetNullableType.IsValueType && (underlyingType == null || underlyingType != targetNullableType))
e56040d0a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-05 15:09:59 +0200 1080)         {
e56040d0a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-05 15:09:59 +0200 1081)             var nullableType = StaticType.Nullable.MakeGenericType(targetNullableType);
e56040d0a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-05 15:09:59 +0200 1082)             var constructor = nullableType.GetConstructor(new[] { targetNullableType });
5197d27f3 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (sergergood            2022-12-03 22:00:02 +0300 1083)             expression = TypeCache.Get(expression.Type).IsNullable() ? expression :
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1084)                 Expression.New(constructor, ConvertExpressionToTypeExpression(expression, targetNullableType));
e56040d0a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-05 15:09:59 +0200 1085)         }
e56040d0a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-05 15:09:59 +0200 1086) 
11df3563a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 14:15:02 +0200 1087)         return expression;
e56040d0a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-05 15:09:59 +0200 1088)     }
e56040d0a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-05 15:09:59 +0200 1089) 
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1090)     /// <summary>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1091)     ///
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1092)     /// </summary>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1093)     /// <param name="expression"></param>
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100 1094)     /// <param name="trueToType"></param>
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1095)     /// <returns></returns>
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1096)     internal static Expression ConvertExpressionWithAutomaticConversion(Expression expression,
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100 1097)         Type trueToType)
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1098)     {
d4e90c09c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (sergergood            2022-12-08 21:20:16 +0300 1099)         var fromType = TypeCache.Get(expression.Type).GetUnderlyingType();
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100 1100)         var toType = TypeCache.Get(trueToType)?.GetUnderlyingType();
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1101) 
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1102)         // Guid to String
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1103)         if (fromType == StaticType.Guid && toType == StaticType.String)
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1104)         {
84fe19e2b src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2025-02-12 14:25:47 +0100 1105)             var result = ConvertExpressionToGuidToStringExpression(expression);
84fe19e2b src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2025-02-12 14:25:47 +0100 1106) 
84fe19e2b src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2025-02-12 14:25:47 +0100 1107)             if (fromType != expression.Type)
84fe19e2b src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2025-02-12 14:25:47 +0100 1108)             {
84fe19e2b src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2025-02-12 14:25:47 +0100 1109)                 // Handle nullability
84fe19e2b src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2025-02-12 14:25:47 +0100 1110)                 expression = Expression.Condition(
84fe19e2b src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2025-02-12 14:25:47 +0100 1111)                     Expression.Property(expression, nameof(Nullable<Guid>.HasValue)),
84fe19e2b src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2025-02-12 14:25:47 +0100 1112)                     result,
84fe19e2b src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2025-02-12 14:25:47 +0100 1113)                     Expression.Constant(null, StaticType.String)
84fe19e2b src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2025-02-12 14:25:47 +0100 1114)                     );
84fe19e2b src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2025-02-12 14:25:47 +0100 1115)             }
84fe19e2b src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2025-02-12 14:25:47 +0100 1116)             else
84fe19e2b src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2025-02-12 14:25:47 +0100 1117)                 expression = result;
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1118)         }
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1119) 
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1120)         // String to Guid
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1121)         else if (fromType == StaticType.String && toType == StaticType.Guid)
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1122)         {
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1123)             expression = ConvertExpressionToStringToGuidExpression(expression);
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1124)         }
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1125) 
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1126)         // TimeSpan to DateTime
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1127)         else if (fromType == StaticType.TimeSpan && toType == StaticType.DateTime)
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1128)         {
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1129)             expression = ConvertExpressionToTimeSpanToDateTimeExpression(expression);
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1130)         }
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1131) 
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1132)         // DateTime to TimeSpan
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1133)         else if (fromType == StaticType.DateTime && toType == StaticType.TimeSpan)
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1134)         {
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1135)             expression = ConvertExpressionToDateTimeToTimeSpanExpression(expression);
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1136)         }
306e54f78 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 07:41:37 +0100 1137) #if NET
4b02b04fd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Andrii Chebukin       2023-09-13 23:22:26 +0400 1138)         // DateTime to DateOnly
4b02b04fd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Andrii Chebukin       2023-09-13 23:22:26 +0400 1139)         else if (fromType == StaticType.DateTime && toType == StaticType.DateOnly)
4b02b04fd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Andrii Chebukin       2023-09-13 23:22:26 +0400 1140)         {
4b02b04fd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Andrii Chebukin       2023-09-13 23:22:26 +0400 1141)             expression = ConvertExpressionToDateTimeToDateOnlyExpression(expression);
4b02b04fd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Andrii Chebukin       2023-09-13 23:22:26 +0400 1142)         }
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200 1143) 
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200 1144)         // DateOnly to DateTime
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200 1145)         else if (fromType == StaticType.DateOnly && toType == StaticType.DateTime)
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200 1146)         {
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200 1147)             expression = ConvertExpressionToDateOnlyToDateTimeExpression(expression);
e94685469 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-10-20 20:54:24 +0200 1148)         }
a1467a9b8 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Andrii Chebukin       2023-09-13 23:52:10 +0400 1149) #endif
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1150)         // Others
b94dfcf33 src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2025-02-03 14:10:54 +0100 1151)         else if (toType != StaticType.SqlVariant)
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1152)         {
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100 1153)             expression = ConvertExpressionToSystemConvertExpression(expression, trueToType);
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1154)         }
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1155) 
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1156)         // Return
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1157)         return expression;
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1158)     }
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1159) 
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1160)     /// <summary>
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1161)     ///
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1162)     /// </summary>
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1163)     /// <param name="expression"></param>
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1164)     /// <param name="readerExpression"></param>
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1165)     /// <param name="handlerInstance"></param>
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1166)     /// <param name="classPropertyParameterInfo"></param>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1167)     /// <returns></returns>
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1168)     internal static Expression ConvertExpressionToPropertyHandlerGetExpression(Expression expression,
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1169)         Expression readerExpression,
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1170)         object handlerInstance,
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1171)         ClassPropertyParameterInfo classPropertyParameterInfo)
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1172)     {
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1173)         // Return if null
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1174)         if (handlerInstance == null)
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1175)         {
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1176)             return expression;
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1177)         }
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1178) 
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1179)         // Variables Needed
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1180)         var getMethod = GetPropertyHandlerGetMethod(handlerInstance);
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1181)         var getParameter = GetPropertyHandlerGetParameter(getMethod);
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1182) 
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1183)         // Call the PropertyHandler.Get
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1184)         expression = Expression.Call(Expression.Constant(handlerInstance), getMethod, new[]
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1185)         {
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1186)             ConvertExpressionToTypeExpression(expression, getParameter.ParameterType),
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1187)             CreatePropertyHandlerGetOptionsExpression(readerExpression, classPropertyParameterInfo?.ClassProperty)
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1188)         });
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1189) 
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1190)         // Convert to the return type
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1191)         return ConvertExpressionToTypeExpression(expression, getMethod.ReturnType);
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1192)     }
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1193) 
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 1194)     /// <summary>
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 1195)     ///
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 1196)     /// </summary>
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1197)     /// <typeparam name="TResult"></typeparam>
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1198)     /// <param name="entityExpression"></param>
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1199)     /// <param name="readerParameterExpression"></param>
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 1200)     /// <returns></returns>
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1201)     internal static Expression ConvertExpressionToClassHandlerGetExpression<TResult>(Expression entityExpression,
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1202)         ParameterExpression readerParameterExpression)
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1203)     {
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1204)         var typeOfResult = typeof(TResult);
df7555581 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-17 06:20:43 +0200 1205) 
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1206)         // Check the handler
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1207)         var handlerInstance = GetClassHandler(typeOfResult);
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1208)         if (handlerInstance == null)
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1209)         {
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1210)             return entityExpression;
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1211)         }
df7555581 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-17 06:20:43 +0200 1212) 
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1213)         // Validate
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1214)         var handlerType = handlerInstance.GetType();
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1215)         if (handlerType.IsClassHandlerValidForModel(typeOfResult) == false)
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1216)         {
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1217)             throw new InvalidTypeException($"The class handler '{handlerType.FullName}' cannot be used for the type '{typeOfResult.FullName}'.");
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1218)         }
df7555581 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-17 06:20:43 +0200 1219) 
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1220)         // Call the ClassHandler.Get method
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1221)         var getMethod = GetClassHandlerGetMethod(handlerInstance);
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1222)         return Expression.Call(Expression.Constant(handlerInstance),
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1223)             getMethod,
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1224)             entityExpression,
11bfcff6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-19 22:41:12 +0200 1225)             CreateClassHandlerGetOptionsExpression(readerParameterExpression));
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 1226)     }
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1227) 
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1228)     /// <summary>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1229)     ///
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1230)     /// </summary>
11df3563a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 14:15:02 +0200 1231)     /// <param name="expression"></param>
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1232)     /// <param name="parameterExpression"></param>
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1233)     /// <param name="classProperty"></param>
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1234)     /// <param name="targetType"></param>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1235)     /// <returns></returns>
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1236)     internal static Expression ConvertExpressionToPropertyHandlerSetExpression(Expression expression,
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1237)         Expression parameterExpression,
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1238)         ClassProperty classProperty,
952ab4650 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-02-14 23:51:20 +0100 1239)         Type targetType) =>
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1240)         ConvertExpressionToPropertyHandlerSetExpressionTuple(expression, parameterExpression, classProperty, targetType).convertedExpression;
67fa9b374 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (ESB16687              2020-12-23 18:58:02 +0800 1241) 
67fa9b374 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (ESB16687              2020-12-23 18:58:02 +0800 1242)     /// <summary>
67fa9b374 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (ESB16687              2020-12-23 18:58:02 +0800 1243)     ///
67fa9b374 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (ESB16687              2020-12-23 18:58:02 +0800 1244)     /// </summary>
67fa9b374 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (ESB16687              2020-12-23 18:58:02 +0800 1245)     /// <param name="expression"></param>
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1246)     /// <param name="parameterExpression"></param>
67fa9b374 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (ESB16687              2020-12-23 18:58:02 +0800 1247)     /// <param name="classProperty"></param>
67fa9b374 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (ESB16687              2020-12-23 18:58:02 +0800 1248)     /// <param name="targetType"></param>
67fa9b374 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (ESB16687              2020-12-23 18:58:02 +0800 1249)     /// <returns></returns>
650434c56 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (fredliex              2021-01-18 00:45:48 +0800 1250)     internal static (Expression convertedExpression, Type handlerSetReturnType) ConvertExpressionToPropertyHandlerSetExpressionTuple(Expression expression,
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1251)         Expression parameterExpression,
67fa9b374 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (ESB16687              2020-12-23 18:58:02 +0800 1252)         ClassProperty classProperty,
b35386fcd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (fredliex              2020-12-25 10:01:02 +0800 1253)         Type targetType)
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1254)     {
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1255)         var handlerInstance = classProperty?.GetPropertyHandler() ??
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1256)             PropertyHandlerCache.Get<object>(targetType);
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1257) 
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1258)         // Check
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1259)         if (handlerInstance == null)
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1260)         {
b35386fcd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (fredliex              2020-12-25 10:01:02 +0800 1261)             return (expression, null);
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1262)         }
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1263) 
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1264)         // Variables
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1265)         var setMethod = GetPropertyHandlerSetMethod(handlerInstance);
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1266)         var setParameter = GetPropertyHandlerSetParameter(setMethod);
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1267) 
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1268)         // Nullable
2d0bbb3b6 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-08 00:49:48 +0100 1269)         expression = ConvertExpressionToNullableExpression(expression,
d4e90c09c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (sergergood            2022-12-08 21:20:16 +0300 1270)             TypeCache.Get(setParameter.ParameterType).GetUnderlyingType() ?? targetType);
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1271) 
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1272)         // Call
6f25b30ec RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-30 12:14:14 +0100 1273)         var valueExpression = ConvertExpressionToTypeExpression(expression, setParameter.ParameterType);
11df3563a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 14:15:02 +0200 1274)         expression = Expression.Call(Expression.Constant(handlerInstance),
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1275)             setMethod,
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1276)             new[]
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1277)             {
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1278)                 valueExpression,
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1279)                 CreatePropertyHandlerSetOptionsExpression(parameterExpression,classProperty)
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1280)             });
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1281) 
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1282)         // Align
b35386fcd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (fredliex              2020-12-25 10:01:02 +0800 1283)         return (ConvertExpressionToTypeExpression(expression, setMethod.ReturnType), setMethod.ReturnType);
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1284)     }
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1285) 
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1286)     /// <summary>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1287)     ///
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1288)     /// </summary>
11bfcff6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-19 22:41:12 +0200 1289)     /// <param name="commandExpression"></param>
256411987 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-05 11:48:13 +0100 1290)     /// <param name="resultType"></param>
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1291)     /// <param name="entityOrEntitiesExpression"></param>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1292)     /// <returns></returns>
11bfcff6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-19 22:41:12 +0200 1293)     internal static Expression ConvertExpressionToClassHandlerSetExpression(Expression commandExpression,
11bfcff6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-19 22:41:12 +0200 1294)         Type resultType,
256411987 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-05 11:48:13 +0100 1295)         Expression entityOrEntitiesExpression)
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1296)     {
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1297)         // Check the handler
256411987 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-05 11:48:13 +0100 1298)         var handlerInstance = GetClassHandler(resultType);
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1299)         if (handlerInstance == null)
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1300) 
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1301)         {
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1302)             return entityOrEntitiesExpression;
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1303)         }
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1304) 
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1305)         // Validate
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1306)         var handlerType = handlerInstance.GetType();
256411987 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-05 11:48:13 +0100 1307)         if (handlerType.IsClassHandlerValidForModel(resultType) == false)
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1308)         {
256411987 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-05 11:48:13 +0100 1309)             throw new InvalidTypeException($"The class handler '{handlerType.FullName}' cannot be used for type '{resultType.FullName}'.");
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1310)         }
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1311) 
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1312)         // Call the IClassHandler.Set method
11bfcff6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-19 22:41:12 +0200 1313)         //var typeOfListEntity = typeof(IList<>).MakeGenericType(StaticType.Object);
11bfcff6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-19 22:41:12 +0200 1314)         //var type = typeOfListEntity.IsAssignableFrom(entityOrEntitiesExpression.Type) ?
11bfcff6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-19 22:41:12 +0200 1315)         //    typeOfListEntity : resultType;
11bfcff6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-19 22:41:12 +0200 1316)         var setMethod = GetClassHandlerSetMethod(handlerInstance);
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1317)         entityOrEntitiesExpression = Expression.Call(Expression.Constant(handlerInstance),
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1318)             setMethod,
11bfcff6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-19 22:41:12 +0200 1319)             ConvertExpressionToTypeExpression(entityOrEntitiesExpression, resultType),
11bfcff6a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-19 22:41:12 +0200 1320)             CreateClassHandlerSetOptionsExpression(commandExpression));
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1321) 
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1322)         // Return the block
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1323)         return entityOrEntitiesExpression;
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1324)     }
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1325) 
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1326)     #endregion
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1327) 
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1328)     #region Common
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1329) 
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200 1330)     /// <summary>
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200 1331)     ///
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200 1332)     /// </summary>
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200 1333)     /// <param name="expression"></param>
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200 1334)     /// <param name="enumType"></param>
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200 1335)     /// <returns></returns>
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200 1336)     internal static Expression GetEnumIsDefinedExpression(Expression expression,
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200 1337)         Type enumType)
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200 1338)     {
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200 1339)         var parameters = new Expression[]
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200 1340)         {
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200 1341)             Expression.Constant(enumType),
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200 1342)             ConvertExpressionToTypeExpression(expression, StaticType.Object)
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200 1343)         };
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200 1344)         return Expression.Call(GetEnumIsDefinedMethod(), parameters);
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200 1345)     }
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200 1346) 
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200 1347)     /// <summary>
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200 1348)     ///
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200 1349)     /// </summary>
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200 1350)     /// <param name="expression"></param>
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200 1351)     /// <param name="enumType"></param>
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200 1352)     /// <returns></returns>
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200 1353)     internal static Expression GetEnumGetNameExpression(Expression expression,
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200 1354)         Type enumType)
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200 1355)     {
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200 1356)         var parameters = new Expression[]
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200 1357)         {
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200 1358)             Expression.Constant(enumType),
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200 1359)             ConvertExpressionToTypeExpression(expression, StaticType.Object)
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200 1360)         };
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200 1361)         return Expression.Call(GetEnumGetNameMethod(), parameters);
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200 1362)     }
1e4189455 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-24 22:00:05 +0200 1363) 
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1364)     /// <summary>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1365)     ///
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1366)     /// </summary>
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1367)     /// <param name="readerParameterExpression"></param>
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1368)     /// <param name="classPropertyParameterInfo"></param>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1369)     /// <param name="readerField"></param>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1370)     /// <returns></returns>
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1371)     internal static Expression GetClassPropertyParameterInfoValueExpression(ParameterExpression readerParameterExpression,
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1372)         ClassPropertyParameterInfo classPropertyParameterInfo,
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100 1373)         DataReaderField readerField, IDbSetting dbSetting)
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1374)     {
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1375)         // False expression
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1376)         var falseExpression = GetClassPropertyParameterInfoIsDbNullFalseValueExpression(readerParameterExpression,
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100 1377)             classPropertyParameterInfo, readerField, dbSetting);
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1378) 
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1379)         // Skip if possible
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1380)         if (readerField?.DbField?.IsNullable == false)
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1381)         {
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1382)             return falseExpression;
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1383)         }
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1384) 
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1385)         // IsDbNull Check
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1386)         var isDbNullExpression = Expression.Call(readerParameterExpression,
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1387)             StaticType.DbDataReader.GetMethod("IsDBNull"), Expression.Constant(readerField.Ordinal));
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1388) 
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1389)         // True Expression
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1390)         var trueExpression = GetClassPropertyParameterInfoIsDbNullTrueValueExpression(readerParameterExpression,
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1391)             classPropertyParameterInfo, readerField);
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1392) 
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1393)         // Set the value
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1394)         return Expression.Condition(isDbNullExpression, trueExpression, falseExpression);
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1395)     }
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1396) 
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1397)     /// <summary>
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1398)     ///
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1399)     /// </summary>
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1400)     /// <param name="readerExpression"></param>
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1401)     /// <param name="classPropertyParameterInfo"></param>
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1402)     /// <param name="readerField"></param>
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100 1403)     ///
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1404)     /// <returns></returns>
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1405)     internal static Expression GetClassPropertyParameterInfoIsDbNullTrueValueExpression(Expression readerExpression,
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1406)         ClassPropertyParameterInfo classPropertyParameterInfo,
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1407)         DataReaderField readerField)
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1408)     {
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1409)         var parameterType = GetPropertyHandlerGetParameter(classPropertyParameterInfo)?.ParameterType;
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1410)         var classPropertyParameterInfoType = classPropertyParameterInfo.GetTargetType();
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1411) 
67fa9b374 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (ESB16687              2020-12-23 18:58:02 +0800 1412)         // get handler on class property or type level. for detect default value type and convert
67fa9b374 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (ESB16687              2020-12-23 18:58:02 +0800 1413)         var handlerInstance = GetHandlerInstance(classPropertyParameterInfo, readerField) ?? PropertyHandlerCache.Get<object>(classPropertyParameterInfo.GetTargetType());
67fa9b374 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (ESB16687              2020-12-23 18:58:02 +0800 1414) 
67fa9b374 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (ESB16687              2020-12-23 18:58:02 +0800 1415)         // default value expression
67fa9b374 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (ESB16687              2020-12-23 18:58:02 +0800 1416)         var valueType = handlerInstance == null ?
67fa9b374 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (ESB16687              2020-12-23 18:58:02 +0800 1417)             parameterType ?? classPropertyParameterInfoType :
67fa9b374 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (ESB16687              2020-12-23 18:58:02 +0800 1418)             GetPropertyHandlerGetParameter(GetPropertyHandlerGetMethod(handlerInstance)).ParameterType;
67fa9b374 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (ESB16687              2020-12-23 18:58:02 +0800 1419)         Expression valueExpression = Expression.Default(valueType);
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1420) 
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1421)         // Property Handler
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1422)         try
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1423)         {
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1424)             valueExpression = ConvertExpressionToPropertyHandlerGetExpression(valueExpression, readerExpression, handlerInstance, classPropertyParameterInfo);
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1425)         }
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1426)         catch (Exception ex)
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1427)         {
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1428)             throw new InvalidOperationException($"Compiler.DataReader.IsDbNull.TrueExpression: Failed to convert the value expression for property handler '{handlerInstance?.GetType()}'. " +
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1429)                 $"{classPropertyParameterInfo.GetDescriptiveContextString()}", ex);
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1430)         }
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1431) 
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1432)         // Align the type
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1433)         try
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1434)         {
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1435)             valueExpression = ConvertExpressionToTypeExpression(valueExpression, classPropertyParameterInfoType);
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1436)         }
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1437)         catch (Exception ex)
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1438)         {
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1439)             throw new InvalidOperationException($"Compiler.DataReader.IsDbNull.TrueExpression: Failed to convert the value expression into its destination .NET CLR Type '{classPropertyParameterInfoType.FullName}'. " +
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1440)                 $"{classPropertyParameterInfo.GetDescriptiveContextString()}", ex);
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1441)         }
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1442) 
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1443)         // Return
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1444)         return valueExpression;
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1445)     }
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1446) 
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1447)     /// <summary>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1448)     ///
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1449)     /// </summary>
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1450)     /// <param name="readerParameterExpression"></param>
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1451)     /// <param name="classPropertyParameterInfo"></param>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1452)     /// <param name="readerField"></param>
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100 1453)     /// <param name="dbSetting"></param>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1454)     /// <returns></returns>
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1455)     internal static Expression GetClassPropertyParameterInfoIsDbNullFalseValueExpression(ParameterExpression readerParameterExpression,
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1456)         ClassPropertyParameterInfo classPropertyParameterInfo,
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100 1457)         DataReaderField readerField,
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100 1458)         IDbSetting dbSetting)
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1459)     {
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1460)         var parameterType = GetPropertyHandlerGetParameter(classPropertyParameterInfo)?.ParameterType;
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1461)         var classPropertyParameterInfoType = classPropertyParameterInfo.GetTargetType();
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1462)         var targetType = parameterType ?? classPropertyParameterInfoType;
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1463)         var readerGetValueMethod = GetDbReaderGetValueOrDefaultMethod(readerField);
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1464)         var valueExpression = (Expression)GetDbReaderGetValueExpression(readerParameterExpression,
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1465)             readerGetValueMethod, readerField.Ordinal);
d4e90c09c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (sergergood            2022-12-08 21:20:16 +0300 1466)         var targetTypeUnderlyingType = TypeCache.Get(targetType).GetUnderlyingType();
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1467) 
67fa9b374 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (ESB16687              2020-12-23 18:58:02 +0800 1468)         // get handler on class property or type level
67fa9b374 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (ESB16687              2020-12-23 18:58:02 +0800 1469)         var handlerInstance = GetHandlerInstance(classPropertyParameterInfo, readerField) ?? PropertyHandlerCache.Get<object>(classPropertyParameterInfo.GetTargetType());
67fa9b374 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (ESB16687              2020-12-23 18:58:02 +0800 1470) 
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1471)         // Enumerations
67fa9b374 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (ESB16687              2020-12-23 18:58:02 +0800 1472)         if (targetTypeUnderlyingType.IsEnum)
67fa9b374 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (ESB16687              2020-12-23 18:58:02 +0800 1473)         {
1f7f928aa RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-08-28 21:57:14 +0200 1474)             // If it has a PropertyHandler and the parameter type is matching, then, skip the auto conversion.
67fa9b374 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (ESB16687              2020-12-23 18:58:02 +0800 1475)             var autoConvertEnum = true;
67fa9b374 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (ESB16687              2020-12-23 18:58:02 +0800 1476)             if (handlerInstance != null)
67fa9b374 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (ESB16687              2020-12-23 18:58:02 +0800 1477)             {
1f7f928aa RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-08-28 21:57:14 +0200 1478)                 var getParameter = GetPropertyHandlerGetParameter(GetPropertyHandlerGetMethod(handlerInstance));
d4e90c09c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (sergergood            2022-12-08 21:20:16 +0300 1479)                 autoConvertEnum = !(TypeCache.Get(getParameter.ParameterType).GetUnderlyingType() == readerField.Type);
67fa9b374 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (ESB16687              2020-12-23 18:58:02 +0800 1480)             }
67fa9b374 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (ESB16687              2020-12-23 18:58:02 +0800 1481)             if (autoConvertEnum)
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1482)             {
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1483)                 try
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1484)                 {
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100 1485)                     valueExpression = ConvertExpressionToEnumExpression(valueExpression, readerField.Type, targetTypeUnderlyingType, dbSetting);
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1486)                 }
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1487)                 catch (Exception ex)
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1488)                 {
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1489)                     throw new InvalidOperationException($"Compiler.DataReader.IsDbNull.FalseExpression: Failed to convert the value expression into enum type '{targetType.GetUnderlyingType()}'. " +
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1490)                         $"{classPropertyParameterInfo.GetDescriptiveContextString()}", ex);
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1491)                 }
67fa9b374 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (ESB16687              2020-12-23 18:58:02 +0800 1492) 
67fa9b374 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (ESB16687              2020-12-23 18:58:02 +0800 1493)             }
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1494)         }
076f07d24 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-22 22:12:09 +0200 1495)         else
076f07d24 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-22 22:12:09 +0200 1496)         {
076f07d24 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-22 22:12:09 +0200 1497)             // Auto-conversion
076f07d24 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-22 22:12:09 +0200 1498)             try
076f07d24 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-22 22:12:09 +0200 1499)             {
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100 1500)                 valueExpression = ConvertExpressionWithAutomaticConversion(valueExpression, targetType);
076f07d24 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-22 22:12:09 +0200 1501)             }
076f07d24 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-22 22:12:09 +0200 1502)             catch (Exception ex)
076f07d24 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-22 22:12:09 +0200 1503)             {
076f07d24 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-22 22:12:09 +0200 1504)                 throw new InvalidOperationException($"Compiler.DataReader.IsDbNull.FalseExpression: Failed to automatically convert the value expression. " +
076f07d24 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-22 22:12:09 +0200 1505)                     $"{classPropertyParameterInfo.GetDescriptiveContextString()}", ex);
076f07d24 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-22 22:12:09 +0200 1506)             }
076f07d24 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-22 22:12:09 +0200 1507)         }
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1508) 
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1509)         // Property Handler
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1510)         try
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1511)         {
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1512)             valueExpression = ConvertExpressionToPropertyHandlerGetExpression(
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1513)                 valueExpression, readerParameterExpression, handlerInstance, classPropertyParameterInfo);
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1514)         }
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1515)         catch (Exception ex)
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1516)         {
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1517)             throw new InvalidOperationException($"Compiler.DataReader.IsDbNull.FalseExpression: Failed to convert the value expression for property handler '{handlerInstance?.GetType()}'. " +
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1518)                 $"{classPropertyParameterInfo.GetDescriptiveContextString()}", ex);
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1519)         }
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1520) 
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1521)         // Align the type
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1522)         try
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1523)         {
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1524)             valueExpression = ConvertExpressionToTypeExpression(valueExpression, classPropertyParameterInfoType);
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1525)         }
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1526)         catch (Exception ex)
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1527)         {
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1528)             throw new InvalidOperationException($"Compiler.DataReader.IsDbNull.FalseExpression: Failed to convert the value expression into its destination .NET CLR Type '{classPropertyParameterInfoType.FullName}'. " +
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1529)                 $"{classPropertyParameterInfo.GetDescriptiveContextString()}", ex);
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1530)         }
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1531) 
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1532)         // Return
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1533)         return valueExpression;
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1534)     }
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1535) 
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1536)     /// <summary>
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1537)     ///
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1538)     /// </summary>
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1539)     /// <param name="targetType"></param>
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1540)     /// <returns></returns>
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1541)     internal static Expression GetNullableTypeExpression(Type targetType) =>
15309a0cd src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2025-02-12 10:03:30 +0100 1542)         targetType.IsValueType
15309a0cd src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2025-02-12 10:03:30 +0100 1543)         ? Expression.New(StaticType.Nullable.MakeGenericType(TypeCache.Get(targetType).GetUnderlyingType()))
15309a0cd src/RepoDb/Reflection/Compiler/Compiler.cs         (Bert Huijben          2025-02-12 10:03:30 +0100 1544)         : Expression.Constant(null, targetType);
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1545) 
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1546)     /// <summary>
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1547)     ///
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1548)     /// </summary>
491ca1aeb RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-05 07:57:08 +0200 1549)     /// <typeparam name="TResult"></typeparam>
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1550)     /// <param name="readerFieldsName"></param>
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1551)     /// <param name="dbSetting"></param>
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1552)     /// <returns></returns>
491ca1aeb RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-05 07:57:08 +0200 1553)     internal static IEnumerable<ClassPropertyParameterInfo> GetClassPropertyParameterInfos<TResult>(IEnumerable<string> readerFieldsName,
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1554)         IDbSetting dbSetting)
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1555)     {
491ca1aeb RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-05 07:57:08 +0200 1556)         var typeOfResult = typeof(TResult);
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1557)         var list = new List<ClassPropertyParameterInfo>();
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1558) 
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1559)         // Parameter information
491ca1aeb RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-05 07:57:08 +0200 1560)         var constructorInfo = typeOfResult.GetConstructorWithMostArguments();
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1561)         var parameterInfos = constructorInfo?.GetParameters().AsList();
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1562) 
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1563)         // Class properties
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1564)         var classProperties = PropertyCache
491ca1aeb RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-05 07:57:08 +0200 1565)             .Get(typeOfResult)?
d7b6efb91 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-12-04 16:27:24 +0100 1566)             //.Where(property => property.PropertyInfo.CanWrite)
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1567)             .Where(property =>
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1568)                 readerFieldsName?.FirstOrDefault(field =>
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1569)                     string.Equals(field.AsUnquoted(true, dbSetting), property.GetMappedName().AsUnquoted(true, dbSetting), StringComparison.OrdinalIgnoreCase)) != null)
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1570)             .AsList();
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1571) 
fae5f84be RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-20 15:00:43 +0200 1572)         // ParameterInfos
d7b6efb91 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-12-04 16:27:24 +0100 1573)         parameterInfos?
d7b6efb91 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-12-04 16:27:24 +0100 1574)             .ForEach(parameterInfo =>
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1575)             {
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1576)                 var classProperty = classProperties?.
d7b6efb91 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-12-04 16:27:24 +0100 1577)                     FirstOrDefault(property =>
d7b6efb91 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-12-04 16:27:24 +0100 1578)                         string.Equals(property.PropertyInfo.Name, parameterInfo.Name, StringComparison.OrdinalIgnoreCase));
1d1615df5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Simon Cropp           2021-01-19 20:38:21 +1100 1579)                 if (classProperty != null)
1d1615df5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Simon Cropp           2021-01-19 20:38:21 +1100 1580)                 {
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1581)                     list.Add(new ClassPropertyParameterInfo
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1582)                     {
952ab4650 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-02-14 23:51:20 +0100 1583)                         ClassProperty = classProperty, //classProperty.PropertyInfo.CanWrite ? classProperty : null,
d7b6efb91 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-12-04 16:27:24 +0100 1584)                         ParameterInfo = parameterInfo,
d7b6efb91 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-12-04 16:27:24 +0100 1585)                         ParameterInfoMappedClassProperty = classProperty
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1586)                     });
1d1615df5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Simon Cropp           2021-01-19 20:38:21 +1100 1587)                 }
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1588)             });
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1589) 
fae5f84be RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-20 15:00:43 +0200 1590)         // ClassProperties
d7b6efb91 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-12-04 16:27:24 +0100 1591)         classProperties
d7b6efb91 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-12-04 16:27:24 +0100 1592)             .Where(property => property.PropertyInfo.CanWrite)
d7b6efb91 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-12-04 16:27:24 +0100 1593)             .AsList()
d7b6efb91 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-12-04 16:27:24 +0100 1594)             .ForEach(property =>
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1595)             {
d7b6efb91 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-12-04 16:27:24 +0100 1596)                 var listItem = list.FirstOrDefault(item => item.ClassProperty == property);
e906181ae RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 15:32:46 +0200 1597)                 if (listItem != null)
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1598)                 {
fae5f84be RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-20 15:00:43 +0200 1599)                     return;
e906181ae RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 15:32:46 +0200 1600)                 }
d7b6efb91 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-12-04 16:27:24 +0100 1601)                 list.Add(new ClassPropertyParameterInfo { ClassProperty = property });
fae5f84be RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-20 15:00:43 +0200 1602)             });
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1603) 
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1604)         // Return the list
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1605)         return list;
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1606)     }
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1607) 
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1608)     /// <summary>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1609)     /// Returns the list of the bindings for the entity.
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1610)     /// </summary>
491ca1aeb RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-05 07:57:08 +0200 1611)     /// <typeparam name="TResult">The target entity type.</typeparam>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1612)     /// <param name="readerParameterExpression">The data reader parameter.</param>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1613)     /// <param name="readerFields">The list of fields to be bound from the data reader.</param>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1614)     /// <param name="dbSetting">The database setting that is being used.</param>
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1615)     /// <returns>The enumerable list of <see cref="MemberBinding"/> objects.</returns>
491ca1aeb RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-05 07:57:08 +0200 1616)     internal static IEnumerable<MemberBinding> GetMemberBindingsForDataEntity<TResult>(ParameterExpression readerParameterExpression,
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1617)         IEnumerable<DataReaderField> readerFields,
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1618)         IDbSetting dbSetting)
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1619)     {
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1620)         // Variables needed
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1621)         var readerFieldsName = readerFields.Select(f => f.Name.ToLowerInvariant()).AsList();
491ca1aeb RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-05 07:57:08 +0200 1622)         var classPropertyParameterInfos = GetClassPropertyParameterInfos<TResult>(readerFieldsName, dbSetting);
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1623) 
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1624)         // Check the presence
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1625)         if (classPropertyParameterInfos?.Any() != true)
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1626)         {
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1627)             return default;
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1628)         }
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1629) 
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1630)         // Variables needed
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1631)         var memberBindings = new List<MemberBinding>();
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1632) 
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1633)         // Iterate each properties
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1634)         foreach (var classPropertyParameterInfo in classPropertyParameterInfos)
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1635)         {
d7b6efb91 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-12-04 16:27:24 +0100 1636)             var mappedName = classPropertyParameterInfo.ParameterInfoMappedClassProperty?.GetMappedName().AsUnquoted(true, dbSetting) ??
d7b6efb91 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-12-04 16:27:24 +0100 1637)                 classPropertyParameterInfo.ParameterInfo?.Name.AsUnquoted(true, dbSetting) ??
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1638)                 classPropertyParameterInfo.ClassProperty?.GetMappedName().AsUnquoted(true, dbSetting);
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1639) 
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1640)             // Skip if not found
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1641)             var ordinal = readerFieldsName.IndexOf(mappedName?.ToLowerInvariant());
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1642)             if (ordinal < 0)
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1643)             {
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1644)                 continue;
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1645)             }
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1646) 
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1647)             // Get the value expression
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1648)             var readerField = readerFields.First(f => string.Equals(f.Name.AsUnquoted(true, dbSetting), mappedName.AsUnquoted(true, dbSetting), StringComparison.OrdinalIgnoreCase));
11df3563a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 14:15:02 +0200 1649)             var expression = GetClassPropertyParameterInfoValueExpression(readerParameterExpression,
975e0d89b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-22 18:58:56 +0100 1650)                 classPropertyParameterInfo, readerField, dbSetting);
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1651) 
5c810a733 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-10 11:57:54 +0200 1652)             try
5c810a733 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-10 11:57:54 +0200 1653)             {
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1654)                 // Member values
952ab4650 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-02-14 23:51:20 +0100 1655)                 var memberAssignment = classPropertyParameterInfo.ClassProperty?.PropertyInfo?.CanWrite == true ?
11df3563a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 14:15:02 +0200 1656)                     Expression.Bind(classPropertyParameterInfo.ClassProperty.PropertyInfo, expression) : null;
11df3563a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 14:15:02 +0200 1657)                 var argument = classPropertyParameterInfo.ParameterInfo != null ? expression : null;
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1658) 
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1659)                 // Add the bindings
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1660)                 memberBindings.Add(new MemberBinding
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1661)                 {
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1662)                     ClassProperty = classPropertyParameterInfo.ClassProperty,
d1c088f76 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-02-06 15:07:37 +0100 1663)                     ParameterInfo = classPropertyParameterInfo?.ParameterInfo,
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1664)                     MemberAssignment = memberAssignment,
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1665)                     Argument = argument
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1666)                 });
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1667)             }
5c810a733 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-10 11:57:54 +0200 1668)             catch (Exception ex)
5c810a733 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-10 11:57:54 +0200 1669)             {
5c810a733 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-10 11:57:54 +0200 1670)                 throw new InvalidOperationException($"Compiler.MemberBinding: Failed to bind the value expression into a property/ctor-argument. " +
5c810a733 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-10 11:57:54 +0200 1671)                     $"{classPropertyParameterInfo.GetDescriptiveContextString()}", ex);
5c810a733 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-10 11:57:54 +0200 1672)             }
5c810a733 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-10 11:57:54 +0200 1673)         }
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1674) 
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1675)         // Return the value
26d62906d RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-22 14:29:36 +0200 1676)         return memberBindings;
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1677)     }
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1678) 
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1679)     /// <summary>
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1680)     ///
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1681)     /// </summary>
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1682)     /// <param name="readerParameterExpression"></param>
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1683)     /// <param name="ordinal"></param>
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1684)     /// <returns></returns>
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1685)     internal static Expression GetDbNullExpression(ParameterExpression readerParameterExpression,
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1686)         int ordinal) =>
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1687)         GetDbNullExpression(readerParameterExpression, Expression.Constant(ordinal));
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1688) 
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1689)     /// <summary>
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1690)     ///
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1691)     /// </summary>
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1692)     /// <param name="readerParameterExpression"></param>
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1693)     /// <param name="ordinalExpression"></param>
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1694)     /// <returns></returns>
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1695)     internal static Expression GetDbNullExpression(ParameterExpression readerParameterExpression,
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1696)         ConstantExpression ordinalExpression) =>
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1697)         Expression.Call(readerParameterExpression, StaticType.DbDataReader.GetMethod("IsDBNull"), ordinalExpression);
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1698) 
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1699)     /// <summary>
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1700)     ///
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1701)     /// </summary>
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1702)     /// <param name="readerParameterExpression"></param>
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1703)     /// <param name="readerGetValueMethod"></param>
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1704)     /// <param name="ordinal"></param>
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1705)     /// <returns></returns>
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1706)     internal static MethodCallExpression GetDbReaderGetValueExpression(ParameterExpression readerParameterExpression,
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1707)         MethodInfo readerGetValueMethod,
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1708)         int ordinal) =>
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1709)         GetDbReaderGetValueExpression(readerParameterExpression, readerGetValueMethod, Expression.Constant(ordinal));
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1710) 
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1711)     /// <summary>
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1712)     ///
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1713)     /// </summary>
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1714)     /// <param name="readerParameterExpression"></param>
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1715)     /// <param name="readerGetValueMethod"></param>
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1716)     /// <param name="ordinalExpression"></param>
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1717)     /// <returns></returns>
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1718)     internal static MethodCallExpression GetDbReaderGetValueExpression(ParameterExpression readerParameterExpression,
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1719)         MethodInfo readerGetValueMethod,
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1720)         ConstantExpression ordinalExpression) =>
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1721)         Expression.Call(readerParameterExpression, readerGetValueMethod, ordinalExpression);
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1722) 
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1723)     /// <summary>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1724)     /// Returns the list of the bindings for the object.
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1725)     /// </summary>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1726)     /// <param name="readerParameterExpression">The data reader parameter.</param>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1727)     /// <param name="readerFields">The list of fields to be bound from the data reader.</param>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1728)     /// <returns>The enumerable list of child elements initializations.</returns>
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1729)     internal static IEnumerable<ElementInit> GetMemberBindingsForDictionary(ParameterExpression readerParameterExpression,
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1730)         IList<DataReaderField> readerFields)
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1731)     {
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1732)         // Initialize variables
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1733)         var elementInits = new List<ElementInit>();
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1734)         var addMethod = StaticType.IDictionaryStringObject.GetMethod("Add", new[] { StaticType.String, StaticType.Object });
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1735) 
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1736)         // Iterate each properties
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1737)         for (var ordinal = 0; ordinal < readerFields?.Count; ordinal++)
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1738)         {
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1739)             var readerField = readerFields[ordinal];
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1740)             var readerGetValueMethod = GetDbReaderGetValueOrDefaultMethod(readerField);
11df3563a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 14:15:02 +0200 1741)             var expression = (Expression)GetDbReaderGetValueExpression(readerParameterExpression, readerGetValueMethod, ordinal);
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1742) 
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1743)             // Check for nullables
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1744)             if (readerField.DbField == null || readerField.DbField?.IsNullable == true)
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1745)             {
2c4a9677b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 15:50:36 +0200 1746)                 var isDbNullExpression = GetDbNullExpression(readerParameterExpression, ordinal);
7d1c5279b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-16 16:53:16 +0200 1747)                 var toType = (readerField.Type?.IsValueType != true) ? (readerField.Type ?? StaticType.Object) : StaticType.Object;
11df3563a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 14:15:02 +0200 1748)                 expression = Expression.Condition(isDbNullExpression, Expression.Default(toType),
11df3563a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 14:15:02 +0200 1749)                     ConvertExpressionToTypeExpression(expression, toType));
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1750)             }
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1751) 
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1752)             // Add to the bindings
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1753)             var values = new Expression[]
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1754)             {
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1755)                 Expression.Constant(readerField.Name),
11df3563a RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 14:15:02 +0200 1756)                 ConvertExpressionToTypeExpression(expression, StaticType.Object)
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1757)             };
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1758)             elementInits.Add(Expression.ElementInit(addMethod, values));
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1759)         }
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1760) 
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1761)         // Return the result
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1762)         return elementInits;
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1763)     }
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 1764) 
6d07a1842 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-10 06:46:43 +0200 1765)     /// <summary>
6d07a1842 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-10 06:46:43 +0200 1766)     ///
6d07a1842 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-10 06:46:43 +0200 1767)     /// </summary>
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1768)     /// <param name="entityInstanceExpression"></param>
6d07a1842 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-10 06:46:43 +0200 1769)     /// <param name="classProperty"></param>
6d07a1842 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-10 06:46:43 +0200 1770)     /// <param name="dbField"></param>
6d07a1842 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-10 06:46:43 +0200 1771)     /// <returns></returns>
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1772)     internal static Expression GetEntityInstancePropertyValueExpression(Expression entityInstanceExpression,
6d07a1842 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-10 06:46:43 +0200 1773)         ClassProperty classProperty,
6d07a1842 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-10 06:46:43 +0200 1774)         DbField dbField)
6d07a1842 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-10 06:46:43 +0200 1775)     {
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1776)         var expression = (Expression)Expression.Property(entityInstanceExpression, classProperty.PropertyInfo);
6d07a1842 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-10 06:46:43 +0200 1777) 
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1778)         // Target type
d4e90c09c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (sergergood            2022-12-08 21:20:16 +0300 1779)         var handlerInstance = classProperty.GetPropertyHandler() ?? PropertyHandlerCache.Get<object>(TypeCache.Get(dbField.Type).GetUnderlyingType());
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100 1780)         var targetType = GetPropertyHandlerSetParameter(handlerInstance)?.ParameterType ?? dbField.TypeNullable();
6d07a1842 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-10 06:46:43 +0200 1781) 
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1782)         /*
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1783)          * Note: The other data provider can coerce the Enum into its destination data type in the DB by default,
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1784)          *       except for PostgreSQL. The code written below is only to address the issue for this specific provider.
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1785)          */
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1786) 
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1787)         // Enum Handling
d4e90c09c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (sergergood            2022-12-08 21:20:16 +0300 1788)         if (TypeCache.Get(classProperty.PropertyInfo.PropertyType).GetUnderlyingType().IsEnum == true)
9b96fca8b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:57:55 +0200 1789)         {
9b96fca8b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:57:55 +0200 1790)             try
e3cee7350 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-09-10 13:52:52 +0200 1791)             {
8f7cda981 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-09-13 18:15:18 +0200 1792)                 if (!IsPostgreSqlUserDefined(dbField))
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1793)                 {
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100 1794)                     var enumType = TypeCache.Get(classProperty.PropertyInfo.PropertyType).GetUnderlyingType();
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100 1795)                     var dbType = classProperty.GetDbType() ?? enumType.GetDbType();
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100 1796)                     var toType = dbType.HasValue ? new DbTypeToClientTypeResolver().Resolve(dbType.Value) : targetType;
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100 1797) 
5c25223ff RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-27 16:07:09 +0100 1798)                     expression = ConvertEnumExpressionToTypeExpression(expression, toType);
feb4f87b4 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:09:55 +0200 1799)                 }
e3cee7350 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-09-10 13:52:52 +0200 1800)             }
9b96fca8b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:57:55 +0200 1801)             catch (Exception ex)
9b96fca8b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:57:55 +0200 1802)             {
9b96fca8b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:57:55 +0200 1803)                 throw new InvalidOperationException($"Compiler.Entity/Object.Property: Failed to convert the value expression from " +
9b96fca8b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:57:55 +0200 1804)                     $"enumeration '{classProperty.PropertyInfo.PropertyType.FullName}' to type '{targetType?.GetUnderlyingType()}'. {classProperty}", ex);
076f07d24 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-22 22:12:09 +0200 1805)             }
076f07d24 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-22 22:12:09 +0200 1806)         }
076f07d24 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-22 22:12:09 +0200 1807) 
076f07d24 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-22 22:12:09 +0200 1808)         // Auto-conversion Handling
076f07d24 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-22 22:12:09 +0200 1809)         try
076f07d24 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-22 22:12:09 +0200 1810)         {
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100 1811)             var origExpression = expression;
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100 1812)             expression = ConvertExpressionWithAutomaticConversion(expression, targetType);
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100 1813) 
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100 1814)             if (dbField?.IsIdentity == true
000000000 src/RepoDb/Reflection/Compiler/Compiler.cs         (Not Committed Yet     2025-02-16 11:02:49 +0100 1815)                 && targetType.IsValueType && TypeCache.Get(targetType).IsNullable()
f5ada6663 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-24 17:55:19 +0100 1816)                 && TypeCache.Get(origExpression.Type).IsNullable())
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100 1817)             {
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100 1818)                 var nullableType = typeof(Nullable<>).MakeGenericType(expression.Type);
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100 1819) 
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100 1820)                 // Don't set '0' in the identity output property
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100 1821)                 expression = Expression.Condition(
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100 1822)                     Expression.Property(origExpression, nameof(Nullable<int>.HasValue)),
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100 1823)                     Expression.Convert(expression, nullableType),
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100 1824)                     Expression.Constant(null, nullableType));
ed2465b64 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-13 08:00:06 +0100 1825)             }
076f07d24 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-22 22:12:09 +0200 1826)         }
076f07d24 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-22 22:12:09 +0200 1827)         catch (Exception ex)
076f07d24 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-22 22:12:09 +0200 1828)         {
076f07d24 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-22 22:12:09 +0200 1829)             throw new InvalidOperationException($"Compiler.Entity/Object.Property: Failed to automatically convert the value expression for " +
076f07d24 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-10-22 22:12:09 +0200 1830)                 $"property '{classProperty.GetMappedName()} ({classProperty.PropertyInfo.PropertyType.FullName})'. {classProperty}", ex);
9b96fca8b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:57:55 +0200 1831)         }
6d07a1842 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-10 06:46:43 +0200 1832) 
6d07a1842 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-10 06:46:43 +0200 1833)         // Property Handler
9b96fca8b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:57:55 +0200 1834)         try
9b96fca8b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:57:55 +0200 1835)         {
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1836)             expression = ConvertExpressionToPropertyHandlerSetExpression(
d4e90c09c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (sergergood            2022-12-08 21:20:16 +0300 1837)                 expression, null, classProperty, TypeCache.Get(dbField?.Type).GetUnderlyingType());
9b96fca8b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:57:55 +0200 1838)         }
9b96fca8b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:57:55 +0200 1839)         catch (Exception ex)
9b96fca8b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:57:55 +0200 1840)         {
9b96fca8b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:57:55 +0200 1841)             throw new InvalidOperationException($"Compiler.Entity/Object.Property: Failed to convert the value expression for property handler '{handlerInstance?.GetType()}'. " +
9b96fca8b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:57:55 +0200 1842)                 $"{classProperty}", ex);
9b96fca8b RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-09 16:57:55 +0200 1843)         }
71cda1890 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 15:24:07 +0200 1844) 
712eecab3 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-02-20 23:15:39 +0100 1845)         // Return the Value
712eecab3 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-02-20 23:15:39 +0100 1846)         return ConvertExpressionToTypeExpression(expression, StaticType.Object);
71cda1890 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 15:24:07 +0200 1847)     }
71cda1890 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 15:24:07 +0200 1848) 
e3cee7350 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-09-10 13:52:52 +0200 1849)     /// <summary>
e3cee7350 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-09-10 13:52:52 +0200 1850)     ///
e3cee7350 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-09-10 13:52:52 +0200 1851)     /// </summary>
e3cee7350 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-09-10 13:52:52 +0200 1852)     /// <param name="dbField"></param>
e3cee7350 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-09-10 13:52:52 +0200 1853)     /// <returns></returns>
8f7cda981 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-09-13 18:15:18 +0200 1854)     private static bool IsPostgreSqlUserDefined(DbField dbField) =>
8f7cda981 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-09-13 18:15:18 +0200 1855)         string.Equals(dbField?.DatabaseType, "USER-DEFINED", StringComparison.OrdinalIgnoreCase) &&
8f7cda981 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-09-13 18:15:18 +0200 1856)         string.Equals(dbField?.Provider, "PGSQL", StringComparison.OrdinalIgnoreCase);
e3cee7350 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-09-10 13:52:52 +0200 1857) 
41762f956 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-09-10 16:37:04 +0200 1858)     /// <summary>
41762f956 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-09-10 16:37:04 +0200 1859)     ///
41762f956 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-09-10 16:37:04 +0200 1860)     /// </summary>
41762f956 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-09-10 16:37:04 +0200 1861)     /// <param name="size"></param>
41762f956 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-09-10 16:37:04 +0200 1862)     /// <param name="dbField"></param>
41762f956 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-09-10 16:37:04 +0200 1863)     private static int GetSize(int? size,
41762f956 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-09-10 16:37:04 +0200 1864)         DbField dbField) =>
41762f956 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-09-10 16:37:04 +0200 1865)         size.HasValue ? size.Value :
41762f956 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-09-10 16:37:04 +0200 1866)              dbField?.Size.HasValue == true ? dbField.Size.Value : default;
41762f956 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-09-10 16:37:04 +0200 1867) 
46c096722 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 16:12:57 +0200 1868)     /// <summary>
46c096722 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 16:12:57 +0200 1869)     ///
46c096722 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 16:12:57 +0200 1870)     /// </summary>
6f3a27f0e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 10:51:33 +0200 1871)     /// <param name="propertyExpression"></param>
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1872)     /// <param name="objectInstanceExpression"></param>
46c096722 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 16:12:57 +0200 1873)     /// <param name="dbField"></param>
46c096722 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 16:12:57 +0200 1874)     /// <returns></returns>
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1875)     internal static Expression GetObjectInstancePropertyValueExpression(ParameterExpression propertyExpression,
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1876)         Expression objectInstanceExpression,
46c096722 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 16:12:57 +0200 1877)         DbField dbField)
71cda1890 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 15:24:07 +0200 1878)     {
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1879)         var methodInfo = StaticType.PropertyInfo.GetMethod("GetValue", new[] { StaticType.Object });
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1880)         var expression = (Expression)Expression.Call(propertyExpression, methodInfo, objectInstanceExpression);
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1881) 
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1882)         // Property Handler
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1883)         expression = ConvertExpressionToPropertyHandlerSetExpression(expression,
d4e90c09c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (sergergood            2022-12-08 21:20:16 +0300 1884)             null, null, TypeCache.Get(dbField?.Type).GetUnderlyingType());
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1885) 
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1886)         // Convert to object
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1887)         return ConvertExpressionToTypeExpression(expression, StaticType.Object);
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1888)     }
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1889) 
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1890)     /// <summary>
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1891)     ///
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1892)     /// </summary>
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1893)     /// <param name="dictionaryInstanceExpression"></param>
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1894)     /// <param name="dbField"></param>
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1895)     /// <returns></returns>
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1896)     internal static Expression GetDictionaryStringObjectPropertyValueExpression(Expression dictionaryInstanceExpression,
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1897)         DbField dbField)
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1898)     {
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1899)         var methodInfo = StaticType.IDictionaryStringObject.GetMethod("get_Item", new[] { StaticType.String });
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1900)         var expression = (Expression)Expression.Call(dictionaryInstanceExpression, methodInfo, Expression.Constant(dbField.Name));
71cda1890 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 15:24:07 +0200 1901) 
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1902)         // Property Handler
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 1903)         expression = ConvertExpressionToPropertyHandlerSetExpression(expression,
d4e90c09c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (sergergood            2022-12-08 21:20:16 +0300 1904)             null, null, TypeCache.Get(dbField.Type).GetUnderlyingType());
71cda1890 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 15:24:07 +0200 1905) 
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1906)         // Convert to object
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1907)         return ConvertExpressionToTypeExpression(expression, StaticType.Object);
46c096722 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 16:12:57 +0200 1908)     }
46c096722 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 16:12:57 +0200 1909) 
46c096722 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 16:12:57 +0200 1910)     /// <summary>
46c096722 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 16:12:57 +0200 1911)     ///
46c096722 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 16:12:57 +0200 1912)     /// </summary>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 1913)     /// <param name="dbParameterExpression"></param>
6f3a27f0e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 10:51:33 +0200 1914)     /// <param name="entityExpression"></param>
6f3a27f0e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 10:51:33 +0200 1915)     /// <param name="propertyExpression"></param>
46c096722 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 16:12:57 +0200 1916)     /// <param name="classProperty"></param>
46c096722 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 16:12:57 +0200 1917)     /// <param name="dbField"></param>
46c096722 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 16:12:57 +0200 1918)     /// <param name="dbSetting"></param>
46c096722 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 16:12:57 +0200 1919)     /// <returns></returns>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 1920)     internal static Expression GetDataEntityDbParameterValueAssignmentExpression(ParameterExpression dbParameterExpression,
6f3a27f0e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 10:51:33 +0200 1921)         Expression entityExpression,
6f3a27f0e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 10:51:33 +0200 1922)         ParameterExpression propertyExpression,
46c096722 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 16:12:57 +0200 1923)         ClassProperty classProperty,
46c096722 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 16:12:57 +0200 1924)         DbField dbField,
46c096722 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 16:12:57 +0200 1925)         IDbSetting dbSetting)
46c096722 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 16:12:57 +0200 1926)     {
062b2a868 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-11 14:02:49 +0100 1927)         Expression expression;
46c096722 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 16:12:57 +0200 1928) 
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1929)         // Get the property value
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1930)         if (propertyExpression.Type == StaticType.PropertyInfo)
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1931)         {
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1932)             expression = GetObjectInstancePropertyValueExpression(propertyExpression, entityExpression, dbField);
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1933)         }
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1934)         else
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1935)         {
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1936)             expression = GetEntityInstancePropertyValueExpression(entityExpression, classProperty, dbField);
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1937)         }
46c096722 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 16:12:57 +0200 1938) 
f5ada6663 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-24 17:55:19 +0100 1939)         // Nullable? -> Convert to DBNull when necessary
f5ada6663 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Bert Huijben          2024-11-24 17:55:19 +0100 1940)         if (TypeCache.Get(expression.Type) is { } returnType && (returnType.IsClassType() || expression.Type == StaticType.Object || returnType.IsNullable()))
46c096722 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 16:12:57 +0200 1941)         {
96293020f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-02-20 16:55:54 +0100 1942)             expression = ConvertExpressionToDbNullExpression(expression);
71cda1890 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 15:24:07 +0200 1943)         }
71cda1890 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 15:24:07 +0200 1944) 
fa70354ca RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-08 17:07:40 +0200 1945)         // Set the value
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 1946)         return Expression.Call(dbParameterExpression, GetDbParameterValueSetMethod(), expression);
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 1947)     }
71cda1890 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 15:24:07 +0200 1948) 
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1949)     /// <summary>
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1950)     ///
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1951)     /// </summary>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 1952)     /// <param name="dbParameterExpression"></param>
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1953)     /// <param name="dictionaryInstanceExpression"></param>
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1954)     /// <param name="dbField"></param>
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1955)     /// <param name="dbSetting"></param>
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1956)     /// <returns></returns>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 1957)     internal static Expression GetDictionaryStringObjectDbParameterValueAssignmentExpression(ParameterExpression dbParameterExpression,
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1958)         Expression dictionaryInstanceExpression,
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1959)         DbField dbField,
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1960)         IDbSetting dbSetting)
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1961)     {
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1962)         var expression = GetDictionaryStringObjectPropertyValueExpression(dictionaryInstanceExpression, dbField);
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1963) 
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1964)         // Nullable
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1965)         if (dbField?.IsNullable == true)
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1966)         {
96293020f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-02-20 16:55:54 +0100 1967)             expression = ConvertExpressionToDbNullExpression(expression);
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1968)         }
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1969) 
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1970)         // Set the value
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 1971)         return Expression.Call(dbParameterExpression, GetDbParameterValueSetMethod(), expression);
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1972)     }
bc1e8edf5 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-06 15:37:33 +0200 1973) 
062b2a868 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-11 14:02:49 +0100 1974)     /// <summary>
062b2a868 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-11 14:02:49 +0100 1975)     ///
062b2a868 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-11 14:02:49 +0100 1976)     /// </summary>
062b2a868 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-11 14:02:49 +0100 1977)     /// <param name="classProperty"></param>
062b2a868 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-11 14:02:49 +0100 1978)     /// <param name="dbField"></param>
062b2a868 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-11 14:02:49 +0100 1979)     /// <returns></returns>
0f340ddee RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (fredliex              2021-01-04 13:10:41 +0800 1980)     private static DbType? GetDbType(ClassProperty classProperty,
062b2a868 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-11 14:02:49 +0100 1981)         DbField dbField)
4f64ab604 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (fred                  2021-01-02 17:06:50 +0800 1982)     {
062b2a868 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-11 14:02:49 +0100 1983)         var dbType = IsPostgreSqlUserDefined(dbField) ? DbType.Object : classProperty?.GetDbType();
4f64ab604 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (fred                  2021-01-02 17:06:50 +0800 1984)         if (dbType == null)
4f64ab604 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (fred                  2021-01-02 17:06:50 +0800 1985)         {
d4e90c09c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (sergergood            2022-12-08 21:20:16 +0300 1986)             var underlyingType = TypeCache.Get(dbField?.Type)?.GetUnderlyingType();
4f64ab604 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (fred                  2021-01-02 17:06:50 +0800 1987)             dbType = TypeMapper.Get(underlyingType) ?? new ClientTypeToDbTypeResolver().Resolve(underlyingType);
4f64ab604 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (fred                  2021-01-02 17:06:50 +0800 1988)         }
4f64ab604 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (fred                  2021-01-02 17:06:50 +0800 1989)         return dbType;
4f64ab604 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (fred                  2021-01-02 17:06:50 +0800 1990)     }
4f64ab604 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (fred                  2021-01-02 17:06:50 +0800 1991) 
6f3a27f0e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 10:51:33 +0200 1992)     /// <summary>
6f3a27f0e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 10:51:33 +0200 1993)     ///
6f3a27f0e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 10:51:33 +0200 1994)     /// </summary>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 1995)     /// <param name="dbParameterExpression"></param>
5c25223ff RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-27 16:07:09 +0100 1996)     /// <param name="classProperty"></param>
6f3a27f0e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 10:51:33 +0200 1997)     /// <param name="dbField"></param>
6f3a27f0e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 10:51:33 +0200 1998)     /// <returns></returns>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 1999)     internal static MethodCallExpression GetDbParameterDbTypeAssignmentExpression(ParameterExpression dbParameterExpression,
5c25223ff RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-27 16:07:09 +0100 2000)         ClassProperty classProperty,
52fff15c7 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-09-15 00:22:14 +0200 2001)         DbField dbField) =>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2002)         GetDbParameterDbTypeAssignmentExpression(dbParameterExpression, GetDbType(classProperty, dbField));
5c25223ff RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-27 16:07:09 +0100 2003) 
5c25223ff RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-27 16:07:09 +0100 2004)     /// <summary>
5c25223ff RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-27 16:07:09 +0100 2005)     ///
5c25223ff RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-27 16:07:09 +0100 2006)     /// </summary>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2007)     /// <param name="dbParameterExpression"></param>
5c25223ff RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-27 16:07:09 +0100 2008)     /// <param name="dbField"></param>
5c25223ff RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-27 16:07:09 +0100 2009)     /// <returns></returns>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2010)     internal static MethodCallExpression GetDbParameterDbTypeAssignmentExpression(ParameterExpression dbParameterExpression,
52fff15c7 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-09-15 00:22:14 +0200 2011)         DbField dbField) =>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2012)         GetDbParameterDbTypeAssignmentExpression(dbParameterExpression, GetDbType(null, dbField));
5c25223ff RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-27 16:07:09 +0100 2013) 
5c25223ff RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-27 16:07:09 +0100 2014)     /// <summary>
5c25223ff RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-27 16:07:09 +0100 2015)     ///
5c25223ff RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-27 16:07:09 +0100 2016)     /// </summary>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2017)     /// <param name="dbParameterExpression"></param>
5c25223ff RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-27 16:07:09 +0100 2018)     /// <param name="dbType"></param>
5c25223ff RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-27 16:07:09 +0100 2019)     /// <returns></returns>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2020)     internal static MethodCallExpression GetDbParameterDbTypeAssignmentExpression(ParameterExpression dbParameterExpression,
5c25223ff RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-27 16:07:09 +0100 2021)         DbType? dbType)
5c25223ff RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-27 16:07:09 +0100 2022)     {
5c25223ff RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-27 16:07:09 +0100 2023)         var expression = (MethodCallExpression)null;
5c25223ff RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-10-27 16:07:09 +0100 2024) 
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2025)         // Set the DB Type
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2026)         if (dbType != null)
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2027)         {
52fff15c7 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-09-15 00:22:14 +0200 2028)             var dbParameterDbTypeSetMethod = StaticType.DbParameter.GetProperty("DbType").SetMethod;
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2029)             expression = Expression.Call(dbParameterExpression, dbParameterDbTypeSetMethod, Expression.Constant(dbType));
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2030)         }
71cda1890 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 15:24:07 +0200 2031) 
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2032)         // Return the expression
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2033)         return expression;
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2034)     }
71cda1890 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 15:24:07 +0200 2035) 
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2036)     /// <summary>
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2037)     ///
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2038)     /// </summary>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2039)     /// <param name="dbCommandExpression"></param>
786165057 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-11 23:45:58 +0100 2040)     /// <param name="dbField"></param>
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2041)     /// <returns></returns>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2042)     internal static MethodCallExpression GetDbCommandCreateParameterExpression(ParameterExpression dbCommandExpression,
786165057 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-11 23:45:58 +0100 2043)         DbField dbField)
71cda1890 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 15:24:07 +0200 2044)     {
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2045)         var dbCommandCreateParameterMethod = StaticType.DbCommand.GetMethod("CreateParameter");
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2046)         return Expression.Call(dbCommandExpression, dbCommandCreateParameterMethod);
71cda1890 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 15:24:07 +0200 2047)     }
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2048) 
786165057 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-11 23:45:58 +0100 2049)     /// <summary>
786165057 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-11 23:45:58 +0100 2050)     ///
786165057 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-11 23:45:58 +0100 2051)     /// </summary>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2052)     /// <param name="dbParameterExpression"></param>
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2053)     /// <param name="dbField"></param>
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2054)     /// <param name="entityIndex"></param>
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2055)     /// <param name="dbSetting"></param>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2056)     internal static MethodCallExpression GetDbParameterNameAssignmentExpression(Expression dbParameterExpression,
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2057)         DbField dbField,
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2058)         int entityIndex,
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2059)         IDbSetting dbSetting)
71cda1890 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 15:24:07 +0200 2060)     {
c24ec4c17 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-01-08 16:02:58 +0100 2061)         var parameterName = dbField.Name.AsUnquoted(true, dbSetting).AsAlphaNumeric();
7c416618c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (sergergood            2021-05-16 16:20:54 +0300 2062)         parameterName = entityIndex > 0 ? string.Concat(dbSetting.ParameterPrefix, parameterName, "_", entityIndex.ToString()) :
c24ec4c17 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-01-08 16:02:58 +0100 2063)             string.Concat(dbSetting.ParameterPrefix, parameterName);
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2064)         return GetDbParameterNameAssignmentExpression(dbParameterExpression, parameterName);
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2065)     }
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2066) 
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2067)     /// <summary>
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2068)     ///
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2069)     /// </summary>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2070)     /// <param name="dbParameterExpression"></param>
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2071)     /// <param name="parameterName"></param>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2072)     internal static MethodCallExpression GetDbParameterNameAssignmentExpression(Expression dbParameterExpression,
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2073)         string parameterName) =>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2074)         GetDbParameterNameAssignmentExpression(dbParameterExpression, Expression.Constant(parameterName));
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2075) 
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2076)     /// <summary>
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2077)     ///
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2078)     /// </summary>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2079)     /// <param name="dbParameterExpression"></param>
d6bd93605 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 23:30:03 +0200 2080)     /// <param name="paramaterNameExpression"></param>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2081)     internal static MethodCallExpression GetDbParameterNameAssignmentExpression(Expression dbParameterExpression,
d6bd93605 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 23:30:03 +0200 2082)         Expression paramaterNameExpression)
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2083)     {
d6bd93605 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 23:30:03 +0200 2084)         var dbParameterValueNameMethod = StaticType.DbParameter.GetProperty("ParameterName").SetMethod;
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2085)         return Expression.Call(dbParameterExpression, dbParameterValueNameMethod, paramaterNameExpression);
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2086)     }
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2087) 
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2088)     /// <summary>
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2089)     ///
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2090)     /// </summary>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2091)     /// <param name="dbParameterExpression"></param>
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2092)     /// <param name="value"></param>
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2093)     /// <returns></returns>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2094)     internal static MethodCallExpression GetDbParameterValueAssignmentExpression(Expression dbParameterExpression,
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2095)         object value) =>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2096)         GetDbParameterValueAssignmentExpression(dbParameterExpression, Expression.Constant(value));
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2097) 
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2098)     /// <summary>
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2099)     ///
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2100)     /// </summary>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2101)     /// <param name="dbParameterExpression"></param>
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2102)     /// <param name="valueExpression"></param>
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2103)     /// <returns></returns>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2104)     internal static MethodCallExpression GetDbParameterValueAssignmentExpression(Expression dbParameterExpression,
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2105)         Expression valueExpression)
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2106)     {
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2107)         var parameterExpression = ConvertExpressionToTypeExpression(dbParameterExpression, StaticType.DbParameter);
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2108)         var dbParameterValueSetMethod = StaticType.DbParameter.GetProperty("Value").SetMethod;
d6bd93605 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 23:30:03 +0200 2109)         var convertToDbNullMethod = StaticType.Converter.GetMethod("NullToDbNull");
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2110)         return Expression.Call(parameterExpression, dbParameterValueSetMethod,
d6bd93605 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 23:30:03 +0200 2111)             Expression.Call(convertToDbNullMethod, ConvertExpressionToTypeExpression(valueExpression, StaticType.Object)));
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2112)     }
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2113) 
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2114)     /// <summary>
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2115)     ///
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2116)     /// </summary>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2117)     /// <param name="dbParameterExpression"></param>
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2118)     /// <param name="dbType"></param>
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2119)     /// <returns></returns>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2120)     internal static MethodCallExpression GetDbParameterDbTypeAssignmentExpression(Expression dbParameterExpression,
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2121)         DbType dbType) =>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2122)         GetDbParameterDbTypeAssignmentExpression(dbParameterExpression, Expression.Constant(dbType));
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2123) 
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2124)     /// <summary>
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2125)     ///
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2126)     /// </summary>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2127)     /// <param name="dbParameterExpression"></param>
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2128)     /// <param name="dbTypeExpression"></param>
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2129)     /// <returns></returns>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2130)     internal static MethodCallExpression GetDbParameterDbTypeAssignmentExpression(Expression dbParameterExpression,
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2131)         Expression dbTypeExpression)
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2132)     {
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2133)         var parameterExpression = ConvertExpressionToTypeExpression(dbParameterExpression, StaticType.DbParameter);
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2134)         var dbParameterDbTypeSetMethod = StaticType.DbParameter.GetProperty("DbType").SetMethod;
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2135)         return Expression.Call(parameterExpression, dbParameterDbTypeSetMethod, dbTypeExpression);
71cda1890 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 15:24:07 +0200 2136)     }
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2137) 
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2138)     /// <summary>
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2139)     ///
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2140)     /// </summary>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2141)     /// <param name="dbParameterExpression"></param>
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2142)     /// <param name="direction"></param>
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2143)     /// <returns></returns>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2144)     internal static MethodCallExpression GetDbParameterDirectionAssignmentExpression(Expression dbParameterExpression,
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2145)         ParameterDirection direction) =>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2146)         GetDbParameterDirectionAssignmentExpression(dbParameterExpression, Expression.Constant(direction));
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2147) 
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2148)     /// <summary>
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2149)     ///
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2150)     /// </summary>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2151)     /// <param name="dbParameterExpression"></param>
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2152)     /// <param name="directionExpression"></param>
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2153)     /// <returns></returns>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2154)     internal static MethodCallExpression GetDbParameterDirectionAssignmentExpression(Expression dbParameterExpression,
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2155)         Expression directionExpression)
71cda1890 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 15:24:07 +0200 2156)     {
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2157)         var parameterExpression = ConvertExpressionToTypeExpression(dbParameterExpression, StaticType.DbParameter);
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2158)         var dbParameterDirectionSetMethod = StaticType.DbParameter.GetProperty("Direction").SetMethod;
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2159)         return Expression.Call(parameterExpression, dbParameterDirectionSetMethod, directionExpression);
71cda1890 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 15:24:07 +0200 2160)     }
71cda1890 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 15:24:07 +0200 2161) 
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2162)     /// <summary>
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2163)     ///
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2164)     /// </summary>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2165)     /// <param name="dbParameterExpression"></param>
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2166)     /// <param name="size"></param>
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2167)     /// <returns></returns>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2168)     internal static MethodCallExpression GetDbParameterSizeAssignmentExpression(Expression dbParameterExpression,
41762f956 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-09-10 16:37:04 +0200 2169)         int size) =>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2170)         GetDbParameterSizeAssignmentExpression(dbParameterExpression, Expression.Constant(size));
41762f956 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-09-10 16:37:04 +0200 2171) 
41762f956 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-09-10 16:37:04 +0200 2172)     /// <summary>
41762f956 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-09-10 16:37:04 +0200 2173)     ///
41762f956 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-09-10 16:37:04 +0200 2174)     /// </summary>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2175)     /// <param name="dbParameterExpression"></param>
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2176)     /// <param name="sizeExpression"></param>
41762f956 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2021-09-10 16:37:04 +0200 2177)     /// <returns></returns>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2178)     internal static MethodCallExpression GetDbParameterSizeAssignmentExpression(Expression dbParameterExpression,
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2179)         Expression sizeExpression)
71cda1890 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 15:24:07 +0200 2180)     {
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2181)         var parameterExpression = ConvertExpressionToTypeExpression(dbParameterExpression, StaticType.DbParameter);
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2182)         var dbParameterSizeSetMethod = StaticType.DbParameter.GetProperty("Size").SetMethod;
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2183)         return Expression.Call(parameterExpression, dbParameterSizeSetMethod, sizeExpression);
71cda1890 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 15:24:07 +0200 2184)     }
71cda1890 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 15:24:07 +0200 2185) 
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2186)     /// <summary>
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2187)     ///
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2188)     /// </summary>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2189)     /// <param name="dbParameterExpression"></param>
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2190)     /// <param name="precision"></param>
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2191)     /// <returns></returns>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2192)     internal static MethodCallExpression GetDbParameterPrecisionAssignmentExpression(Expression dbParameterExpression,
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2193)         byte precision) =>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2194)         GetDbParameterPrecisionAssignmentExpression(dbParameterExpression, Expression.Constant(precision));
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2195) 
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2196)     /// <summary>
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2197)     ///
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2198)     /// </summary>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2199)     /// <param name="dbParameterExpression"></param>
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2200)     /// <param name="precisionExpression"></param>
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2201)     /// <returns></returns>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2202)     internal static MethodCallExpression GetDbParameterPrecisionAssignmentExpression(Expression dbParameterExpression,
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2203)         Expression precisionExpression)
71cda1890 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 15:24:07 +0200 2204)     {
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2205)         var parameterExpression = ConvertExpressionToTypeExpression(dbParameterExpression, StaticType.DbParameter);
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2206)         var dbParameterPrecisionSetMethod = StaticType.DbParameter.GetProperty("Precision").SetMethod;
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2207)         return Expression.Call(parameterExpression, dbParameterPrecisionSetMethod, precisionExpression);
71cda1890 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 15:24:07 +0200 2208)     }
71cda1890 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 15:24:07 +0200 2209) 
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2210)     /// <summary>
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2211)     ///
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2212)     /// </summary>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2213)     /// <param name="dbParameterExpression"></param>
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2214)     /// <param name="scale"></param>
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2215)     /// <returns></returns>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2216)     internal static MethodCallExpression GetDbParameterScaleAssignmentExpression(Expression dbParameterExpression,
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2217)         byte scale) =>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2218)         GetDbParameterScaleAssignmentExpression(dbParameterExpression, Expression.Constant(scale));
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2219) 
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2220)     /// <summary>
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2221)     ///
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2222)     /// </summary>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2223)     /// <param name="dbParameterExpression"></param>
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2224)     /// <param name="scaleExpression"></param>
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2225)     /// <returns></returns>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2226)     internal static MethodCallExpression GetDbParameterScaleAssignmentExpression(Expression dbParameterExpression,
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2227)         Expression scaleExpression)
71cda1890 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 15:24:07 +0200 2228)     {
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2229)         var parameterExpression = ConvertExpressionToTypeExpression(dbParameterExpression, StaticType.DbParameter);
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2230)         var dbParameterScaleSetMethod = StaticType.DbParameter.GetProperty("Scale").SetMethod;
dd2e6e21c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 15:01:43 +0200 2231)         return Expression.Call(parameterExpression, dbParameterScaleSetMethod, scaleExpression);
71cda1890 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 15:24:07 +0200 2232)     }
71cda1890 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 15:24:07 +0200 2233) 
d6bd93605 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 23:30:03 +0200 2234)     /// <summary>
d6bd93605 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 23:30:03 +0200 2235)     ///
d6bd93605 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 23:30:03 +0200 2236)     /// </summary>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2237)     /// <param name="dbParameterExpression"></param>
d6bd93605 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 23:30:03 +0200 2238)     /// <returns></returns>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2239)     internal static MethodCallExpression EnsureTableValueParameterExpression(Expression dbParameterExpression)
d6bd93605 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 23:30:03 +0200 2240)     {
d6bd93605 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 23:30:03 +0200 2241)         var method = StaticType.DbCommandExtension.GetMethod("EnsureTableValueParameter",
d6bd93605 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 23:30:03 +0200 2242)             System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2243)         return Expression.Call(method, dbParameterExpression);
d6bd93605 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 23:30:03 +0200 2244)     }
d6bd93605 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2022-09-18 23:30:03 +0200 2245) 
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2246)     /// <summary>
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2247)     ///
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2248)     /// </summary>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2249)     /// <param name="dbCommandExpression"></param>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2250)     /// <param name="dbParameterExpression"></param>
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2251)     /// <returns></returns>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2252)     internal static MethodCallExpression GetDbCommandParametersAddExpression(Expression dbCommandExpression,
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2253)         Expression dbParameterExpression)
71cda1890 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 15:24:07 +0200 2254)     {
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2255)         var dbCommandParametersProperty = StaticType.DbCommand.GetProperty("Parameters");
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2256)         var dbParameterCollection = Expression.Property(dbCommandExpression, dbCommandParametersProperty);
9437b7a4f RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-09 14:04:12 +0200 2257)         var dbParameterCollectionAddMethod = StaticType.DbParameterCollection.GetMethod("Add", new[] { StaticType.Object });
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2258)         return Expression.Call(dbParameterCollection, dbParameterCollectionAddMethod, dbParameterExpression);
71cda1890 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 15:24:07 +0200 2259)     }
71cda1890 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 15:24:07 +0200 2260) 
6f3a27f0e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 10:51:33 +0200 2261)     /// <summary>
6f3a27f0e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 10:51:33 +0200 2262)     ///
6f3a27f0e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 10:51:33 +0200 2263)     /// </summary>
6f3a27f0e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 10:51:33 +0200 2264)     /// <param name="dbParameterCollectionExpression"></param>
6f3a27f0e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 10:51:33 +0200 2265)     /// <returns></returns>
6f3a27f0e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 10:51:33 +0200 2266)     internal static Expression GetDbParameterCollectionClearMethodExpression(MemberExpression dbParameterCollectionExpression)
6f3a27f0e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 10:51:33 +0200 2267)     {
6f3a27f0e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 10:51:33 +0200 2268)         var dbParameterCollectionClearMethod = StaticType.DbParameterCollection.GetMethod("Clear");
6f3a27f0e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 10:51:33 +0200 2269)         return Expression.Call(dbParameterCollectionExpression, dbParameterCollectionClearMethod);
6f3a27f0e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 10:51:33 +0200 2270)     }
6f3a27f0e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 10:51:33 +0200 2271) 
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2272)     /// <summary>
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2273)     ///
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2274)     /// </summary>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2275)     /// <param name="dbCommandExpression"></param>
7e0d65efd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-28 08:36:43 +0200 2276)     /// <param name="entityExpression"></param>
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2277)     /// <param name="fieldDirection"></param>
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2278)     /// <param name="entityIndex"></param>
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2279)     /// <param name="dbSetting"></param>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2280)     /// <param name="dbHelper"></param>
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2281)     /// <returns></returns>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2282)     internal static Expression GetPropertyFieldExpression(ParameterExpression dbCommandExpression,
256411987 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-05 11:48:13 +0100 2283)         Expression entityExpression,
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2284)         FieldDirection fieldDirection,
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2285)         int entityIndex,
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2286)         IDbSetting dbSetting,
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2287)         IDbHelper dbHelper)
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2288)     {
7e0d65efd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-28 08:36:43 +0200 2289)         var propertyListExpression = new List<Expression>();
7e0d65efd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-28 08:36:43 +0200 2290)         var propertyVariableListExpression = new List<ParameterExpression>();
7e0d65efd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-28 08:36:43 +0200 2291)         var propertyVariableExpression = (ParameterExpression)null;
7e0d65efd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-28 08:36:43 +0200 2292)         var propertyInstanceExpression = (Expression)null;
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2293)         var classProperty = (ClassProperty)null;
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2294)         var propertyName = fieldDirection.DbField.Name.AsUnquoted(true, dbSetting);
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2295) 
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2296)         // Set the proper assignments (property)
387266563 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (sergergood            2022-12-03 16:19:58 +0300 2297)         if (TypeCache.Get(entityExpression.Type).IsClassType() == false)
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2298)         {
8cc7f2887 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-13 16:38:04 +0200 2299)             var typeGetPropertyMethod = StaticType.Type.GetMethod("GetProperty", new[]
8cc7f2887 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-13 16:38:04 +0200 2300)             {
8cc7f2887 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-13 16:38:04 +0200 2301)                 StaticType.String,
8cc7f2887 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-13 16:38:04 +0200 2302)                 StaticType.BindingFlags
8cc7f2887 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-13 16:38:04 +0200 2303)             });
8cc7f2887 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-13 16:38:04 +0200 2304)             var objectGetTypeMethod = StaticType.Object.GetMethod("GetType");
7e0d65efd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-28 08:36:43 +0200 2305)             propertyVariableExpression = Expression.Variable(StaticType.PropertyInfo, string.Concat("propertyVariable", propertyName));
7e0d65efd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-28 08:36:43 +0200 2306)             propertyInstanceExpression = Expression.Call(Expression.Call(entityExpression, objectGetTypeMethod),
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2307)                 typeGetPropertyMethod, new[]
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2308)                 {
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2309)                     Expression.Constant(propertyName),
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2310)                     Expression.Constant(BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase)
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2311)                 });
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2312)         }
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2313)         else
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2314)         {
7e0d65efd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-28 08:36:43 +0200 2315)             var entityProperties = PropertyCache.Get(entityExpression.Type);
ee2d68980 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-09-06 15:44:59 +0200 2316)             classProperty = entityProperties.FirstOrDefault(property =>
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2317)                 string.Equals(property.GetMappedName().AsUnquoted(true, dbSetting),
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2318)                     propertyName.AsUnquoted(true, dbSetting), StringComparison.OrdinalIgnoreCase));
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2319) 
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2320)             if (classProperty != null)
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2321)             {
7e0d65efd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-28 08:36:43 +0200 2322)                 propertyVariableExpression = Expression.Variable(classProperty.PropertyInfo.PropertyType, string.Concat("propertyVariable", propertyName));
7e0d65efd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-28 08:36:43 +0200 2323)                 propertyInstanceExpression = Expression.Property(entityExpression, classProperty.PropertyInfo);
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2324)             }
256411987 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-05 11:48:13 +0100 2325)             else
256411987 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-05 11:48:13 +0100 2326)             {
256411987 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-05 11:48:13 +0100 2327)                 throw new PropertyNotFoundException($"The property '{propertyName}' is not found from type '{entityExpression.Type}'. The current operation could not proceed.");
256411987 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-05 11:48:13 +0100 2328)             }
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2329)         }
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2330) 
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2331)         // Add the variables
7e0d65efd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-28 08:36:43 +0200 2332)         if (propertyVariableExpression != null && propertyInstanceExpression != null)
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2333)         {
7e0d65efd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-28 08:36:43 +0200 2334)             propertyVariableListExpression.Add(propertyVariableExpression);
7e0d65efd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-28 08:36:43 +0200 2335)             propertyListExpression.Add(Expression.Assign(propertyVariableExpression, propertyInstanceExpression));
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2336) 
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2337)             // Execute the function
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2338)             var parameterAssignment = GetDataEntityParameterAssignmentExpression(dbCommandExpression,
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2339)                 entityIndex,
7e0d65efd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-28 08:36:43 +0200 2340)                 entityExpression,
7e0d65efd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-28 08:36:43 +0200 2341)                 propertyVariableExpression,
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2342)                 fieldDirection.DbField,
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2343)                 classProperty,
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2344)                 fieldDirection.Direction,
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2345)                 dbSetting,
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2346)                 dbHelper);
7e0d65efd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-28 08:36:43 +0200 2347)             propertyListExpression.Add(parameterAssignment);
7e0d65efd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-28 08:36:43 +0200 2348)         }
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2349) 
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2350)         // Add the property block
7e0d65efd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-28 08:36:43 +0200 2351)         return Expression.Block(propertyVariableListExpression, propertyListExpression);
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2352)     }
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2353) 
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2354)     /// <summary>
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2355)     ///
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2356)     /// </summary>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2357)     /// <param name="dbCommandExpression"></param>
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2358)     /// <returns></returns>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2359)     internal static MethodCallExpression GetDbCommandParametersClearExpression(ParameterExpression dbCommandExpression)
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2360)     {
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2361)         var dbParameterCollection = Expression.Property(dbCommandExpression,
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2362)             StaticType.DbCommand.GetProperty("Parameters"));
8cc7f2887 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-13 16:38:04 +0200 2363)         return Expression.Call(dbParameterCollection, StaticType.DbParameterCollection.GetMethod("Clear"));
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2364)     }
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2365) 
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2366)     /// <summary>
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2367)     ///
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2368)     /// </summary>
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2369)     /// <param name="entitiesParameterExpression"></param>
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2370)     /// <param name="typeOfListEntity"></param>
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2371)     /// <param name="entityIndex"></param>
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2372)     /// <returns></returns>
a1dba2363 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 15:21:58 +0200 2373)     internal static MethodCallExpression GetListEntityIndexerExpression(Expression entitiesParameterExpression,
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2374)         Type typeOfListEntity,
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2375)         int entityIndex)
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2376)     {
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2377)         var listIndexerMethod = typeOfListEntity.GetMethod("get_Item", new[] { StaticType.Int32 });
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2378)         return Expression.Call(entitiesParameterExpression, listIndexerMethod,
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2379)             Expression.Constant(entityIndex));
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2380)     }
ec3b53bc9 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-11 07:20:21 +0200 2381) 
7e0d65efd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-28 08:36:43 +0200 2382)     /// <summary>
7e0d65efd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-28 08:36:43 +0200 2383)     ///
7e0d65efd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-28 08:36:43 +0200 2384)     /// </summary>
256411987 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-05 11:48:13 +0100 2385)     /// <param name="resultType"></param>
7e0d65efd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-28 08:36:43 +0200 2386)     /// <param name="expression"></param>
7e0d65efd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-28 08:36:43 +0200 2387)     /// <returns></returns>
256411987 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-05 11:48:13 +0100 2388)     private static Expression ThrowIfNullAfterClassHandlerExpression(Type resultType,
256411987 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-05 11:48:13 +0100 2389)         Expression expression)
7e0d65efd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-28 08:36:43 +0200 2390)     {
7e0d65efd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-28 08:36:43 +0200 2391)         var isNullExpression = Expression.Equal(Expression.Constant(null), expression);
256411987 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-05 11:48:13 +0100 2392)         var exception = new NullReferenceException($"Entity of type '{resultType}' must not be null. If you have defined a class handler, please check the 'Set' method.");
7e0d65efd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-28 08:36:43 +0200 2393)         return Expression.IfThen(isNullExpression, Expression.Throw(Expression.Constant(exception)));
7e0d65efd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-28 08:36:43 +0200 2394)     }
7e0d65efd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-28 08:36:43 +0200 2395) 
a3ec2f9c0 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-20 11:55:28 +0200 2396)     /// <summary>
a3ec2f9c0 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-20 11:55:28 +0200 2397)     ///
a3ec2f9c0 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-20 11:55:28 +0200 2398)     /// </summary>
256411987 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-05 11:48:13 +0100 2399)     /// <param name="entityType"></param>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2400)     /// <param name="dbCommandExpression"></param>
a3ec2f9c0 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-20 11:55:28 +0200 2401)     /// <param name="entitiesParameterExpression"></param>
a3ec2f9c0 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-20 11:55:28 +0200 2402)     /// <param name="fieldDirections"></param>
a3ec2f9c0 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-20 11:55:28 +0200 2403)     /// <param name="entityIndex"></param>
a3ec2f9c0 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-20 11:55:28 +0200 2404)     /// <param name="dbSetting"></param>
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2405)     /// <param name="dbHelper"></param>
a3ec2f9c0 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-20 11:55:28 +0200 2406)     /// <returns></returns>
256411987 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-05 11:48:13 +0100 2407)     private static Expression GetIndexDbParameterSetterExpression(Type entityType,
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2408)         ParameterExpression dbCommandExpression,
a1dba2363 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 15:21:58 +0200 2409)         Expression entitiesParameterExpression,
a3ec2f9c0 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-20 11:55:28 +0200 2410)         IEnumerable<FieldDirection> fieldDirections,
a3ec2f9c0 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-20 11:55:28 +0200 2411)         int entityIndex,
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2412)         IDbSetting dbSetting,
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2413)         IDbHelper dbHelper)
a3ec2f9c0 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-20 11:55:28 +0200 2414)     {
a3ec2f9c0 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-20 11:55:28 +0200 2415)         // Get the current instance
256411987 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-05 11:48:13 +0100 2416)         var entityVariableExpression = Expression.Variable(StaticType.Object, "instance");
256411987 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-05 11:48:13 +0100 2417)         var typeOfListEntity = typeof(IList<>).MakeGenericType(StaticType.Object);
a1dba2363 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 15:21:58 +0200 2418)         var entityParameter = (Expression)GetListEntityIndexerExpression(entitiesParameterExpression, typeOfListEntity, entityIndex);
a3ec2f9c0 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-20 11:55:28 +0200 2419)         var entityExpressions = new List<Expression>();
a3ec2f9c0 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-20 11:55:28 +0200 2420)         var entityVariables = new List<ParameterExpression>();
a3ec2f9c0 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-20 11:55:28 +0200 2421) 
a1dba2363 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 15:21:58 +0200 2422)         // Class handler
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2423)         entityParameter = ConvertExpressionToClassHandlerSetExpression(dbCommandExpression, entityType, entityParameter);
a1dba2363 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 15:21:58 +0200 2424) 
a3ec2f9c0 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-20 11:55:28 +0200 2425)         // Entity instance
7e0d65efd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-28 08:36:43 +0200 2426)         entityVariables.Add(entityVariableExpression);
7e0d65efd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-28 08:36:43 +0200 2427)         entityExpressions.Add(Expression.Assign(entityVariableExpression, entityParameter));
7e0d65efd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-28 08:36:43 +0200 2428) 
7e0d65efd RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-28 08:36:43 +0200 2429)         // Throw if null
256411987 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-05 11:48:13 +0100 2430)         entityExpressions.Add(ThrowIfNullAfterClassHandlerExpression(entityType, entityVariableExpression));
a3ec2f9c0 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-20 11:55:28 +0200 2431) 
a3ec2f9c0 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-20 11:55:28 +0200 2432)         // Iterate the input fields
a3ec2f9c0 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-20 11:55:28 +0200 2433)         foreach (var fieldDirection in fieldDirections)
a3ec2f9c0 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-20 11:55:28 +0200 2434)         {
a3ec2f9c0 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-20 11:55:28 +0200 2435)             // Add the property block
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2436)             var propertyBlock = GetPropertyFieldExpression(dbCommandExpression,
256411987 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-05 11:48:13 +0100 2437)                 ConvertExpressionToTypeExpression(entityVariableExpression, entityType),
256411987 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-05 11:48:13 +0100 2438)                 fieldDirection,
256411987 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2021-12-05 11:48:13 +0100 2439)                 entityIndex,
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2440)                 dbSetting,
a47f26f7c RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Camara Pendon 2023-03-13 22:15:58 +0100 2441)                 dbHelper);
a3ec2f9c0 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-20 11:55:28 +0200 2442) 
a3ec2f9c0 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-20 11:55:28 +0200 2443)             // Add to instance expression
a3ec2f9c0 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-20 11:55:28 +0200 2444)             entityExpressions.Add(propertyBlock);
a3ec2f9c0 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-20 11:55:28 +0200 2445)         }
a3ec2f9c0 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-20 11:55:28 +0200 2446) 
a3ec2f9c0 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-20 11:55:28 +0200 2447)         // Add to the instance block
a3ec2f9c0 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-20 11:55:28 +0200 2448)         return Expression.Block(entityVariables, entityExpressions);
a3ec2f9c0 RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-20 11:55:28 +0200 2449)     }
6f3a27f0e RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-27 10:51:33 +0200 2450) 
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 2451)     #endregion
75df516ce RepoDb.Core/RepoDb/Reflection/Compiler/Compiler.cs (Michael Pendon        2020-08-08 08:11:33 +0200 2452) }
