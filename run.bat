@echo off
setlocal

rem Change to the root directory of your project
cd /d "C:\NSR_Python_Projects\Python_Project_Version2\ETL_Testing_Framework(Config Driven Approach)"

rem Loop through each Python file in Test_Cases and run it
for %%f in (Test_Cases\*.py) do (
    echo Running %%~nf...
    python -m Test_Cases.%%~nf
)

pause
