@echo off
echo Resetting data lake...
if exist lake\ (
    rmdir /s /q lake
)
mkdir lake\bronze
mkdir lake\silver
mkdir lake\gold
echo Lake reset complete.
