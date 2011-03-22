sed "s/0.0.0.0/$1/g" /etc/riaksearch/app.config > /etc/riaksearch/app.config.temp
sed "s/0.0.0.0/$1/g" /etc/riaksearch/vm.args > /etc/riaksearch/vm.args.temp
mv /etc/riaksearch/app.config.temp /etc/riaksearch/app.config
mv /etc/riaksearch/vm.args.temp /etc/riaksearch/vm.args
/usr/sbin/riaksearch-admin reip riak@0.0.0.0 riak@$1
/usr/sbin/riaksearch start