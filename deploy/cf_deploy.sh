#!/usr/bin/env bash
./cf_setenv.sh
pushd ../
rm -rf ibm_graph
cp -r ../py-graph/ibm_graph ./ibm_graph
cf push
rm -rf ibm_graph
ln -s ../py-graph/ibm_graph
popd
while read -r line; do
	param_name=$(echo $line | sed 's/\(^.*\)\=.*$/\1/')
	if [ -n "$param_name" ]
	then
		param_value=$(echo $line | sed 's/^.*\=[ ]*\(.*\)$/\1/')
		cf_setenv_cmd=$(echo cf set-env watson-recipe-bot-graph $param_name $param_value)
        echo $cf_setenv_cmd
		eval $cf_setenv_cmd
	fi
done <<< "$(cat ./.env)"
cf restage watson-recipe-bot-graph
