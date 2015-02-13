#sed "s/0.0.0.0/$1/g" /etc/riaksearch/app.config > /etc/riaksearch/app.config.temp
#sed "s/0.0.0.0/$1/g" /etc/riaksearch/vm.args > /etc/riaksearch/vm.args.temp
#mv /etc/riaksearch/app.config.temp /etc/riaksearch/app.config
#mv /etc/riaksearch/vm.args.temp /etc/riaksearch/vm.args
#/usr/sbin/riaksearch-admin reip riak@0.0.0.0 riak@$1
#/usr/sbin/riaksearch start

sed "s/127.0.0.1/$1/g" /etc/riak/app.config > /etc/riak/app.config.temp
sed "s/127.0.0.1/$1/g" /etc/riak/vm.args > /etc/riak/vm.args.temp
mv /etc/riak/app.config.temp /etc/riak/app.config
mv /etc/riak/vm.args.temp /etc/riak/vm.args
sudo -u riak riak-admin reip riak@127.0.0.1 riak@$1
sudo -u riak riak start