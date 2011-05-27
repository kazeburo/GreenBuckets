return +{
    # listening port of dispatcher
    dispatcher_port => 5000,

    # listening port of worker's status server 
    jobqueue_worker_port => 5101,

    # dispatcher's basic authorization id/pass
    # only used modification methods like PUT,POST,DELETE
    user => 'admin', 
    passwd => 'admin',

    # backend dav storages's basic authorization id/passs
    dav_user => 'storage',
    dav_passwd => 'storage',

    # fontend forwared proxy ip
    front_proxy => [qw!192.168.0.0/16 10.0.0.0/8 127.0.0.1!],

    # dispatcher's status page acl
    dispatcher_status_access => [qw!192.168.0.0/16 10.0.0.0/8 127.0.0.1!],

    # master/slave dbn
    master => ['dbi:mysql:greenbuckets;host=127.0.0.1','user','passwd'],
    slave => [
        ['dbi:mysql:greenbuckets;host=127.0.0.1','user','passwd'],
        ['dbi:mysql:greenbuckets;host=127.0.0.1','user','passwd']
    ],

    # replica number
    replica => 3,

    # number of dispatcher worker
    dispatcher_max_worker => 20,
    # numbe of JobQueue worker
    jobqueue_max_worker => 5,
    recovery_max_worker => 2,

    flat_dav => 1,
#    agent_class => "GreenBuckets::Agent::Dummy",
};

