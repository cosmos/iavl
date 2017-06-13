#!/bin/bash

./start-tendermerk.sh
sleep 5 
./height.sh
./kill-tendermerk.sh
sleep 5 

./restart-tendermerk.sh
sleep 5 
./height.sh
./kill-tendermerk.sh
