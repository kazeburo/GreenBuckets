return +{
    # dispatcher の listenするport
    port => 5000,

    # dispatcher の basic 認証 id/pass
    user => 'admin', 
    passwd => 'admin',

    # dispatcher の許可するアクセス元
    allow_from => [qw!192.168.0.0/16 10.0.0.0/8 127.0.0.1!],

    # proxy 経由の場合そのproxyのip
    front_proxy => [qw!192.168.0.0/16 10.0.0.0/8 127.0.0.1!],

    # master/slave db 同じでも可
    master => ['dbi:mysql:greenbuckets;host=127.0.0.1','user','passwd'],
    slave => ['dbi:mysql:greenbuckets;host=127.0.0.1','user','passwd'],

    # backend の davストレージにアクセスする際のid/pass
    dav_user => 'storage',
    dav_passwd => 'storage'

    # レプリカ数
    replica => 3,

    # Dispatcherのworker数
    dispatcher_worker => 20,
    # JobQueue の worker数
    jobqueue_worker => 5,

};

