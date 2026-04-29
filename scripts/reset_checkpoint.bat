@echo off
echo Resetting checkpoints...
if exist checkpoints\ (
    rmdir /s /q checkpoints
    mkdir checkpoints
    echo Checkpoints cleared.
) else (
    mkdir checkpoints
    echo Checkpoints directory created.
)
