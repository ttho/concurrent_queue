import 'dart:async';
import 'dart:math';
import 'package:concurrent_queue/queue.dart';
import 'package:test/test.dart';
import 'package:concurrent_queue/concurrent_queue.dart';

final _random = new Random();

const fixture = Symbol('fixture');

int randRange(int min, int max) => min + _random.nextInt(max - min);

Future delay(int milliseconds) async => Future.delayed(Duration(milliseconds: milliseconds));


void main() {
  test('.add()', () async {
    var queue = new ConcurrentQueue();

    final future = queue.add<int>(() async => 123);

    expect(queue.size, equals(0));
    expect(queue.pending, equals(1));
    expect(future, completion(equals(123)));
    expect(queue.pendingKeys.isEmpty, isTrue);
  });

  test('.add() - limited concurrency',() async {
    int fixture = 123;
    var queue = new ConcurrentQueue(
      concurrency: 2
    );
    var promise = queue.add( () async => fixture);
    var promise2 = queue.add(() async {
      await Future.delayed(Duration(milliseconds: 300));
      return fixture;
    });
    var promise3 = queue.add(() async => fixture);

    expect(queue.size, equals(1));
    expect(queue.pending, equals(2));
    expect(promise, completion(equals(fixture)));
    expect(promise2, completion(equals(fixture)));
    expect(promise3, completion(equals(fixture)));

  });

  test('.add() - concurrency: 1', () async {
    var input = [
      [10, 300],
      [20, 200],
      [30, 100]
    ];
    var queue = new ConcurrentQueue(concurrency: 1);

    Future<void> mapper (value) => queue.add(() async {
      await Future.delayed(Duration( milliseconds: value[1]));
      return value[0];
    });

    var all = Future.wait(input.map(mapper));

    expect(all, completion(equals([10, 20, 30])));

  });

  test('.add() - concurrency: 5', () async {
    int concurrency = 5;
    var queue = new ConcurrentQueue(concurrency: 5);
    int running = 0;


    var input = List.filled(100, 0).map((val) async {
      queue.add(() async {
        running++;
        expect(running, lessThanOrEqualTo(concurrency));
        expect(queue.pending, lessThanOrEqualTo(concurrency));
        await Future.delayed(Duration(milliseconds: randRange(30, 200)));
        running--;
      });
    });

    await Future.wait(input);
  });

  test('.add() - update concurrency', () async {
    int concurrency = 5;

    var queue = new ConcurrentQueue(
      concurrency: concurrency
    );


    int running = 0;

    var input = List.filled(100, 0).asMap().keys.map((index) async => queue.add(() async {
      running++;
      expect(running, lessThanOrEqualTo(concurrency));
      expect(queue.pending, lessThanOrEqualTo(concurrency));


      int ms = randRange(30, 200);
      await Future.delayed(Duration(milliseconds: ms));
      running--;

      if (index % 30 == 0) {
        queue.concurrency = --concurrency;
        expect(queue.concurrency, concurrency);
      }
    }));

    await Future.wait(input);
  });

  test('.add() - priority', () async {
    List<int> result = <int>[];

    var queue = new ConcurrentQueue(concurrency: 1);

    queue.add(() async => result.add(1), priority: 1);
    queue.add(() async => result.add(0), priority: 0);
    queue.add(() async => result.add(1), priority: 1);
    queue.add(() async => result.add(2), priority: 1);
    queue.add(() async => result.add(3), priority: 2);
    queue.add(() async => result.add(0), priority: -1);
    await queue.onEmpty();

    expect(result, equals([1, 3, 1, 2, 0, 0]));
  });

  test('.addFirst()', () async {
    List<int> result = <int>[];

    var queue = new ConcurrentQueue(concurrency: 1);

    queue.add(() async => result.add(1), priority: 1);
    queue.addFirst(() async => result.add(10)); // first, but previous task is already executed, and queue is empty at that time -> added with priority 0
    queue.add(() async => result.add(0), priority: 0);
    queue.add(() async => result.add(1), priority: 1);
    queue.add(() async => result.add(2), priority: 1);
    queue.add(() async => result.add(3), priority: 2);
    queue.add(() async => result.add(0), priority: -1);
    queue.addFirst(() async => result.add(20)); // second to be executed, as first task was executed immediately after creation
    await queue.onEmpty();

    expect(result, equals([1, 20, 3, 1, 2, 10, 0, 0]));
  });

  test('.onEmpty()', () async{

    var queue = new ConcurrentQueue(concurrency: 1);

    queue.add(() async => 0);
    queue.add(() async => 0);

    expect(queue.size, equals(1));
    expect(queue.pending, equals(1));

    await queue.onEmpty();
    expect(queue.size, equals(0));

    queue.add(() async => 0);
    queue.add(() async => 0);

    expect(queue.size, equals(1));
    expect(queue.pending, equals(1));
    await queue.onEmpty();
    expect(queue.size, equals(0));

    // Test an empty queue
    await queue.onEmpty();
    expect(queue.size, equals(0));
  });

  test('async .onIdle', () async {
    var queue = new ConcurrentQueue(
      concurrency: 2
    );

    List<int> result = [];

    for (int i = 0; i < 4; i += 1) {
      queue.add(() async {
        await Future.delayed(Duration(milliseconds: 60));
        result.add(i);
      });
    }

    queue.start();

    await queue.onIdle();
    expect(result.length, equals(4));
    expect(result, equals([0,1,2,3]));
    expect(queue.size, equals(0));
  });

  test('.onIdle() - no pending', () async {
    var queue = new ConcurrentQueue();
    expect(queue.size, equals(0));
    expect(queue.pending, equals(0));
  });


  test('.clear()', () async {
    var queue = new ConcurrentQueue(
      concurrency: 2,
    );
    queue.add(() async => delay(20000));
    queue.add(() async => delay(20000));
    queue.add(() async => delay(20000));
    queue.add(() async => delay(20000));
    queue.add(() async => delay(20000), key: "1");
    queue.add(() async => delay(20000), key: "2");

    expect(queue.size, equals(4));
    expect(queue.pending, equals(2));
    expect(queue.pendingKeys.isEmpty, isTrue);
    expect(queue.queuedKeys, equals({"2", "1"}));
    queue.clear();
    expect(queue.size, equals(0));
    expect(queue.queuedKeys.isEmpty, isTrue);
  });

  test('.addAll()', () async {
    final queue = ConcurrentQueue();
    final fn = () async => fixture;
    final functions = [fn, fn];
    final promise = queue.addAll(functions);
    expect(queue.size, equals(0));
    expect(queue.pending, equals(2));
    expect(await promise, equals([fixture, fixture]));
  });

  test('autoStart: false', () async {
    final queue = ConcurrentQueue(concurrency: 2, autoStart: false);
    queue.add(() async => delay(20000));
    queue.add(() async => delay(20000));
    queue.add(() async => delay(20000));
    queue.add(() async => delay(20000));
    expect(queue.size, 4);
    expect(queue.pending, 0);
    expect(queue.isPaused, true);

    queue.start();
    expect(queue.size, 2);
    expect(queue.pending, 2);
    expect(queue.isPaused, false);

    queue.clear();
    expect(queue.size, 0);
  });

  test('.start() - return this', () async {
    final queue = ConcurrentQueue(concurrency: 2, autoStart: false);

    queue.add(() async => delay(100));
    queue.add(() async => delay(100));
    queue.add(() async => delay(100));
    expect(queue.size, 3);
    expect(queue.pending, 0);
    await queue.start().onIdle();
    expect(queue.size, 0);
    expect(queue.pending, 0);
  });

  test('.start() - not paused', () async  {
    final queue = ConcurrentQueue();
    expect(queue.isPaused, false);

    queue.start();

    expect(queue.isPaused, false);
  });

  test('.pause()', () {
    final queue = ConcurrentQueue(
      concurrency: 2,
    );

    queue.pause();
    queue.add(() async => delay(20000));
    queue.add(() async => delay(20000));
    queue.add(() async => delay(20000));
    queue.add(() async => delay(20000));
    queue.add(() async => delay(20000));
    expect(queue.size, 5);
    expect(queue.pending, 0);
    expect(queue.isPaused, true);

    queue.start();
    expect(queue.size, 3);
    expect(queue.pending, 2);
    expect(queue.isPaused, false);

    queue.add(() async => delay(20000));
    queue.pause();
    expect(queue.size, 4);
    expect(queue.pending, 2);
    expect(queue.isPaused, true);

    queue.start();
    expect(queue.size, 4);
    expect(queue.pending, 2);
    expect(queue.isPaused, false);

    queue.clear();
    expect(queue.size, 0);
  });

  test('.add() async mixed tasks', () async {
    final queue = ConcurrentQueue(concurrency: 1);
    queue.add(() async => 'sync 1');
    queue.add(() async => delay(1000));
    queue.add(() async => 'sync 2');
    queue.add(() async => fixture);
    expect(queue.size, 3);
    expect(queue.pending, 1);
    await queue.onIdle();
    expect(queue.size, 0);
    expect(queue.pending, 0);
  });

  test('.add() - handle task promise failure', () async {
    final queue = ConcurrentQueue(concurrency: 1);

    expect(() async {
      await queue.add(() async {
        throw Exception();
      });
    }, throwsException);


    queue.add(() async => 'task #1');

    expect(queue.pending, 1);

    await queue.onIdle();

    expect(queue.pending, 0);
  });

  test('.addAll() sync/async mixed tasks', () async {
    final queue = new ConcurrentQueue();

    final functions  = [
      () async => 'sync 1',
      () async => delay(2000),
      () async => 'sync 2',
      () async  => fixture
    ];

    final promise = queue.addAll(functions);

    expect(queue.size, 0);
    expect(queue.pending, 4);
    expect(await promise, equals(['sync 1', null, 'sync 2', fixture]));
  });


  test('should resolve empty when size is zero', () async {
    final queue = ConcurrentQueue(concurrency: 1, autoStart: false);

    // It should take 1 seconds to resolve all tasks
    for (int index = 0; index < 100; index++) {
      queue.add(() async => delay(10));
    }

    (() async {
      await queue.onEmpty();
      expect(queue.size, 0);
    })();

    queue.start();

    // Pause at 0.5 second
    Timer(Duration(milliseconds: 500), () async {
      queue.pause();
      await delay(10);
      queue.start();
    });

    await queue.onIdle();
  });

  test('.add() - throttled', () async {
    final result = <int>[];
    final queue = ConcurrentQueue(
      intervalCap: 1,
      interval: Duration(milliseconds: 500),
      autoStart: false
    );
    queue.add(() async {
      result.add(1);
    });
    queue.start();
    await delay(250);
    queue.add(() async {
      result.add(2);
    });

    expect(result, equals([1]));
    await delay(500);

    await queue.onIdle();
    expect(result, equals([1, 2]));
  });

  test('.add() - throttled, carryoverConcurrencyCount false', () async {
    final result = <int>[];
    final queue = ConcurrentQueue(
      intervalCap: 1,
      carryoverConcurrencyCount: false,
      interval: Duration(milliseconds: 500),
      autoStart: false
    );

    const values = [0, 1];
    values.forEach((value) => queue.add(() async {
      await delay(600);
      result.add(value);
    }));

    queue.start();

    (() async {
      await delay(550);
      expect(queue.pending, 2);
      expect(result, equals([]));
    })();

    (() async {
      await delay(550);
      expect(queue.pending, 2);
      expect(result, equals([]));
    })();

    (() async {
      await delay(650);
      expect(queue.pending, 1);
      expect(result, equals([0]));
    })();

    await delay(1250);
    expect(result, equals(values));
  });

  test('.add() - throttled, carryoverConcurrencyCount true', () async {
    final result = <int>[];
    final queue = ConcurrentQueue(
      intervalCap: 1,
      carryoverConcurrencyCount: true,
      interval: Duration(milliseconds: 500),
      autoStart: false
    );

    const values = <int>[0, 1];

    values.forEach((value) => queue.add(() async {
      await delay(600);
      result.add(value);
    }));

    queue.start();

    (() async {
      await delay(100);
      expect(result, equals([]));
      expect(queue.pending, 1);
    })();

    (() async {
      await delay(550);
      expect(result, equals([]));
      expect(queue.pending, 1);
    })();

    (() async {
      await delay(650);
      expect(result, equals([0]));
      expect(queue.pending, 0);
    })();

    (() async {
      await delay(1550);
      expect(result, equals([0]));
    })();

    await delay(1650);
    expect(result, equals([0, 1]));
  });

  test('.add() - throttled 10, concurrency 5', () async {
    final result = <int>[];
    final queue = ConcurrentQueue(
      concurrency: 5,
      intervalCap: 10,
      interval: Duration(milliseconds: 1000),
      autoStart: false
    );

    final firstValue = [for(var i=0; i<5; i+=1) i];
    final secondValue = [for(var i=0; i<10; i+=1) i];
    final thirdValue = [for(var i=0; i<13; i+=1) i];


    thirdValue.forEach((value) => queue.add(() async {
      await delay(300);
      result.add(value);
    }));

    queue.start();

    expect(result, equals([]));

    (() async {
      await delay(400);

      expect(result, equals(firstValue));
      expect(queue.pending, 5);
    })();

    (() async {
      await delay(700);
      expect(result, equals(secondValue));
    })();

    (() async {
      await delay(1200);
      expect(queue.pending, 3);
      expect(result, equals(secondValue));
    })();

    await delay(1400);
    expect(result, equals(thirdValue));
  });


  test('.add() - throttled finish and resume', () async {
    final result = <int>[];
    final queue = ConcurrentQueue(
      concurrency: 1,
      intervalCap: 2,
      interval: Duration(milliseconds: 2000),
      autoStart: false
    );

    const values = [0, 1];
    const firstValue = [0, 1];
    const secondValue = [0, 1, 2];

    values.forEach((value) => queue.add(() async {
      await delay(100);
      result.add(value);
    }));

    queue.start();

    (() async {
      await delay(1000);
      expect(result, equals(firstValue));

      queue.add(() async {
        await delay(100);
        result.add(2);
      });
    })();

    (() async {
      await delay(1500);
      expect(result, equals(firstValue));
    })();

    await delay(2200);
    expect(result, equals(secondValue));
  });

  test('pause should work when throttled', () async {
    final result = <int>[];
    final queue = ConcurrentQueue(
      concurrency: 2,
      intervalCap: 2,
      interval: Duration(milliseconds: 1000),
      autoStart: false
    );

    const values = [0, 1, 2, 3];
    const firstValue = [0, 1];
    const secondValue = [0, 1, 2, 3];

    values.forEach((value) => queue.add(() async {
      await delay(100);
      result.add(value);
    }));

    queue.start();

    (() async {
      await delay(300);
      expect(result, equals(firstValue));
    })();

    (() async {
      await delay(600);
      queue.pause();
    })();

    (() async {
      await delay(1400);
      expect(result, equals(firstValue));
    })();

    (() async {
      await delay(1500);
      queue.start();
    })();

    (() async {
      await delay(2200);
      expect(result, equals(secondValue));
    })();

    await delay(2500);
  });

  test('clear interval on pause', () async {
    final queue = ConcurrentQueue(
      intervalCap: 2,
      interval: Duration(milliseconds: 100),
    );

    queue.add(() async {
      queue.pause();
    });

    queue.add(() async => 'task #1');

    await delay(300);

    expect(queue.size, 1);
  });

  test('should emit active event per item', () async {
    final items = [0, 1, 2, 3, 4];
    final queue = ConcurrentQueue();

    int eventCount = 0;
    queue.on(QueueEventAction.active, (_) {
      eventCount++;
    });

    for (final item in items) {
      queue.add(() async => item);
    }

    queue.start();
    await queue.onIdle();

    expect(eventCount, items.length);
  });

test('should emit idle event when idle', () async {
	final queue = ConcurrentQueue(concurrency: 1);

	int timesCalled = 0;
	queue.on(QueueEventAction.idle, (_) {
		timesCalled++;
	});

	final job1 = queue.add(() async => delay(100));
	final job2 = queue.add(() async => delay(100));

	expect(queue.pending, 1);
	expect(queue.size, 1);
	expect(timesCalled, 0);

	await job1;

	expect(queue.pending, 1);
	expect(queue.size, 0);
	expect(timesCalled, 0);

	await job2;

	expect(queue.pending, 0);
	expect(queue.size, 0);
	expect(timesCalled, 1);

	final job3 = queue.add(() async => delay(100));

	expect(queue.pending, 1);
	expect(queue.size, 0);
	expect(timesCalled, 1);

	await job3;
	expect(queue.pending, 0);
	expect(queue.size, 0);
	expect(timesCalled, 2);
});

test('should emit add event when adding task', () async {
	final queue = ConcurrentQueue(concurrency: 1);

	int timesCalled = 0;
	queue.on(QueueEventAction.add, (_) {
		timesCalled++;
	});

	final job1 = queue.add(() async => delay(100));

	expect(queue.pending, 1);
	expect(queue.size, 0);
	expect(timesCalled, 1);

	final job2 = queue.add(() async => delay(100));

	expect(queue.pending, 1);
	expect(queue.size, 1);
	expect(timesCalled, 2);

	await job1;

	expect(queue.pending, 1);
	expect(queue.size, 0);
	expect(timesCalled, 2);

	await job2;

	expect(queue.pending, 0);
	expect(queue.size, 0);
	expect(timesCalled, 2);

	final job3 = queue.add(() async => delay(100));

	expect(queue.pending, 1);
	expect(queue.size, 0);
	expect(timesCalled, 3);

	await job3;
	expect(queue.pending, 0);
	expect(queue.size, 0);
	expect(timesCalled, 3);
});

test('should emit next event when completing task', () async {
	final queue = ConcurrentQueue(concurrency: 1);

	int timesCalled = 0;
	queue.on(QueueEventAction.next, (_) {
		timesCalled++;
	});

	final job1 = queue.add(() async => delay(100));

	expect(queue.pending, 1);
	expect(queue.size, 0);
	expect(timesCalled, 0);

	final job2 = queue.add(() async => delay(100));

	expect(queue.pending, 1);
	expect(queue.size, 1);
	expect(timesCalled, 0);

	await job1;

	expect(queue.pending, 1);
	expect(queue.size, 0);
	expect(timesCalled, 1);

	await job2;

	expect(queue.pending, 0);
	expect(queue.size, 0);
	expect(timesCalled, 2);

	final job3 = queue.add(() async => delay(100));

	expect(queue.pending, 1);
	expect(queue.size, 0);
	expect(timesCalled, 2);

	await job3;
	expect(queue.pending, 0);
	expect(queue.size, 0);
	expect(timesCalled, 3);
});

test('should emit completed / error events', () async {
	final queue = ConcurrentQueue(concurrency: 1);

	int errorEvents = 0;
	int completedEvents = 0;
	queue.on(QueueEventAction.error, (_) {
		errorEvents++;
	});
	queue.on(QueueEventAction.completed, (_){
		completedEvents++;
	});

	final job1 = queue.add(() async => delay(100));

	expect(queue.pending, 1);
	expect(queue.size, 0);
	expect(errorEvents, 0);
	expect(completedEvents, 0);

	final job2 = queue.add(() async {
		await delay(1);
		throw Exception('failure');
	});

	expect(queue.pending, 1);
	expect(queue.size, 1);
	expect(errorEvents, 0);
	expect(completedEvents, 0);

	await job1;

	expect(queue.pending, 1);
	expect(queue.size, 0);
	expect(errorEvents, 0);
	expect(completedEvents, 1);

  await expectLater(() async {
    await job2;
  }, throwsException);

	expect(queue.pending, 0);
	expect(queue.size, 0);
	expect(errorEvents, 1);
	expect(completedEvents, 1);

	final job3 = queue.add(() async => delay(100));

	expect(queue.pending, 1);
	expect(queue.size, 0);
	expect(errorEvents, 1);
	expect(completedEvents, 1);

	await job3;
	expect(queue.pending, 0);
	expect(queue.size, 0);
	expect(errorEvents, 1);
	expect(completedEvents, 2);
});


  test('should verify timeout overrides passed to add', () async {
    final queue = ConcurrentQueue(
      throwOnTimeout: true,
      timeout: Duration(milliseconds: 200),
    );

    expect(() => queue.add(() async {
      await delay(400);
    }), throwsException);

    expect(await queue.add(() async {
      await delay(100);
    }), equals(null));

    await queue.onIdle();
  });


  test('.remove() by key', () async {
    var queue = new ConcurrentQueue( concurrency:  1, autoStart: false );

    queue.add<int>(() async => 123);
    queue.add<int>(() async => 123, key: 'deleteMe');
    queue.add<int>(() async => 123);

    expect(queue.size, equals(3));
    expect(queue.pending, equals(0));
    expect(queue.pendingKeys.length, equals(0));
    expect(queue.queuedKeys, equals({"deleteMe"}));

    queue.remove('deleteMe');

    expect(queue.size, equals(2));
    expect(queue.pending, equals(0));
    expect(queue.pendingKeys.length, equals(0));
    expect(queue.queuedKeys.length, equals(0));
  });


  test('.remove() by key while running', () async {
    var queue = new ConcurrentQueue( concurrency:  1 );

    queue.add(() async {
      await delay(300);
      return 123;
    });
    queue.add(() async {
      await delay(300);
    }, key: 'deleteMe');
    queue.add(() async => 123);

    expect(queue.size, equals(2));
    expect(queue.pending, equals(1));
    expect(queue.pendingKeys.length, equals(0));
    expect(queue.queuedKeys, equals({"deleteMe"}));

    final item = queue.remove('deleteMe');

    expect(item, isA<RunFunction>());
    expect(queue.size, equals(1));
    expect(queue.pending, equals(1));
    expect(queue.pendingKeys.length, equals(0));
    expect(queue.queuedKeys.length, equals(0));
  });

  test('.remove() unprocessed key', () async {
    var queue = new ConcurrentQueue( concurrency:  1, autoStart: false );

    queue.add<int>(() async => 123);
    queue.add<int>(() async => 123);
    queue.add<int>(() async => 123);

    expect(queue.size, equals(3));
    expect(queue.pending, equals(0));

    final item = queue.remove('deleteMe');

    expect(item, isNull);
    expect(queue.size, equals(3));
    expect(queue.pending, equals(0));
  });

  test('.remove() should emit remove event', () async {
    var queue = new ConcurrentQueue( concurrency: 1, autoStart: false );
    int removedEvents = 0;

    queue.on(QueueEventAction.remove, (_){
      removedEvents++;
    });

    queue.add<int>(() async => 123);
    queue.add<int>(() async => 123, key: "delete1");
    queue.add<int>(() async => 123, key: "delete2");

    expect(queue.size, equals(3));
    expect(queue.pending, equals(0));
    expect(queue.queuedKeys, equals({"delete1", "delete2"}));

    final item = queue.remove('delete1');
    final item2 = queue.remove('delete2');

    expect(item, isA<RunFunction>());
    expect(item2, isA<RunFunction>());
    expect(queue.size, equals(1));
    expect(queue.pending, equals(0));
    expect(queue.queuedKeys, equals([]));
    expect(removedEvents, 2);
  });

  test('.add() should throw exception when key is already in use', () async {
    var queue = new ConcurrentQueue( concurrency: 1, autoStart: false );

    queue.add<int>(() async => 123);
    queue.add<int>(() async => 123, key: "delete1");
    final task = queue.add<int>(() async => 123, key: "delete1");

    expect(queue.size, equals(2));
    expect(queue.pending, equals(0));

    await expectLater(() async {
      return await task;
    }, throwsException);
  });
}


