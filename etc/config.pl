return +{
    # listen port of dispatcher
    port => 5000,

    # dispatcher's basic authorization id/pass
    user => 'admin', 
    passwd => 'admin',

    # dispatcher's permited ip access from
    allow_from => [qw!192.168.0.0/16 10.0.0.0/8 127.0.0.1!],

    # fontend proxy ip
    front_proxy => [qw!192.168.0.0/16 10.0.0.0/8 127.0.0.1!],

    # master/slave dbn
    master => ['dbi:mysql:greenbuckets;host=127.0.0.1','user','passwd'],
    slave => [
        ['dbi:mysql:greenbuckets;host=127.0.0.2','user','passwd'],
        ['dbi:mysql:greenbuckets;host=127.0.0.3','user','passwd']
    ],

    # backend dav storages's basic authorization id/passs
    dav_user => 'storage',
    dav_passwd => 'storage'

    # replica number
    replica => 3,

    # number of dispatcher worker
    dispatcher_worker => 20,
    # numbe of JobQueue worker
    jobqueue_worker => 5,

};

