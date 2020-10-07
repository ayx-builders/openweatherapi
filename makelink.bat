Pushd "%~dp0"
set tooldir=C:\ProgramData\Alteryx\Tools
mklink /D %tooldir%\open_weather %CD%
