library concurrent_queue;

import 'dart:async';
import 'package:concurrent_queue/queue.dart';
import 'package:rxdart/rxdart.dart';
import 'priority_queue.dart';

export 'priority_queue.dart';

typedef Future<T> Task<T>();

typedef void ResolveFunction<T>();

void _empty() {}

class FutureAndPriority<T> {
  final Future<T> future;
  final int priority;

  FutureAndPriority(this.future, this.priority);
}

class QueueEvent {
  QueueEvent(this.action, {
    this.result,
  });

  QueueEventAction action;
  dynamic result;
}

enum QueueEventAction {
  active,
  idle,
  add,
  next,
  remove,
  completed,
  error,
}

class ConcurrentQueue {

  ConcurrentQueue({
    bool autoStart = true,
    int concurrency = 1<<32,
    Duration interval = Duration.zero,
    int intervalCap = 1<<32,
    bool carryoverConcurrencyCount = false,
    Duration timeout = Duration.zero,
    bool throwOnTimeout = false,
  }) :
    _carryoverConcurrencyCount = carryoverConcurrencyCount,
    _isIntervalIgnored = intervalCap == double.infinity || interval == Duration.zero,
    _timeout = timeout,
    _throwOnTimeout = throwOnTimeout,
    _intervalCap = intervalCap,
    _interval = interval,
    _queue = PriorityQueue(),
    _concurrency = concurrency,
    _isPaused = autoStart == false,
    _eventStream = PublishSubject<QueueEvent>( sync: true ),
    _pendingKeys = {};

  final bool _carryoverConcurrencyCount;

  final bool _isIntervalIgnored;

  int _intervalCount = 0;

  final int _intervalCap;

  Duration? _timeout;

  final bool _throwOnTimeout;

  final Duration _interval;

  DateTime? _intervalEnd;

  Timer? _intervalId;

  Timer? _timeoutId;

  late PriorityQueue _queue;

  final Set<dynamic> _pendingKeys;

  int _pendingCount = 0;

  late int _concurrency;

  bool _isPaused;

  ResolveFunction _resolveEmpty = _empty;

  ResolveFunction _resolveIdle = _empty;

  bool get _doesIntervalAllowAnother {
    return _isIntervalIgnored || _intervalCount < _intervalCap;
  }

  bool get _doesConcurrentAllowAnother {
    return _pendingCount < _concurrency;
  }

  PublishSubject<QueueEvent> _eventStream;

  Stream<QueueEvent> get eventStream => _eventStream.stream;

  void emit(QueueEventAction event, [dynamic result]) {
    _eventStream.add(QueueEvent(event, result: result));
  }

  StreamSubscription on(QueueEventAction action, void Function(QueueEvent) event) {
    return _eventStream
      .where((event) => event.action == action)
      .listen(event);
  }

  void _next() {
    _pendingCount -= 1;
    _tryToStartAnother();
    emit(QueueEventAction.next);
  }

  void _resolvePromises() {
    _resolveEmpty();
    _resolveEmpty = _empty;

    if (_pendingCount == 0) {
      _resolveIdle();
      _resolveIdle = _empty;
      emit(QueueEventAction.idle);
    }
  }

  void _onResumeInterval() {
    _onInterval();
    _initializeIntervalIfNeeded();
    _timeoutId = null;
  }

  bool _isIntervalPaused() {
    DateTime now = DateTime.now();

    if (_intervalId == null) {
      var delay = _intervalEnd != null ? _intervalEnd!.difference(now) : Duration.zero;
      if (delay.inMilliseconds <= 0) {
        // Act as the interval was done
        // We don't need to resume it here because it will be resumed on line 160
        _intervalCount = (_carryoverConcurrencyCount) ? _pendingCount : 0;
      } else {
        // Act as the interval is pending
        if (_timeoutId == null) {
          _timeoutId = Timer(delay, _onResumeInterval);
        }

        return true;
      }
    }

    return false;
  }

  bool _tryToStartAnother() {
    if (_queue.size == 0) {
      if (_intervalId != null) {
        _intervalId!.cancel();
        _intervalId = null;
      }
      _resolvePromises();
      return false;
    }
    if (!_isPaused) {

      bool canInitializeInterval = !_isIntervalIgnored && !_isIntervalPaused();
      if (_doesIntervalAllowAnother && _doesConcurrentAllowAnother) {
        final job = _queue.dequeue();

        if (job == null) {
          return false;
        }

        emit(QueueEventAction.active);
        job();

        if (canInitializeInterval) {
          _initializeIntervalIfNeeded();
        }

        return true;
      }
    }
    return false;
  }

  void _initializeIntervalIfNeeded() {
    if (_isIntervalIgnored || _intervalId != null) {
      return;
    }
    _intervalId = Timer.periodic(_interval, (timer) {
      _onInterval();
    });


    _intervalEnd = DateTime.now().add(_interval);
  }

  void _onInterval() {

    if (_intervalCount == 0 && _pendingCount == 0 && _intervalId != null) {
      _intervalId!.cancel();
      _intervalId = null;
    }

    _intervalCount = _carryoverConcurrencyCount
      ? _pendingCount
      : 0;

    _processQueue();
  }

  void _processQueue() {
    while (_tryToStartAnother()) {}
  }

  int get concurrency {
    return _concurrency;
  }

  set concurrency(int newConcurrency) {
    _concurrency = newConcurrency;
    _processQueue();
  }

  set timeout(Duration timeout) {
    _timeout = timeout;
  }

  Duration get timeout => _timeout!;

  Future<List<T>> addAll<T>(
    List<Task<T>> tasks, {
      int priority = 0,
    }
  ) {
    final waitFor = tasks.map((task) {
      return add(task, priority: priority);
    }).toList();

    return Future.wait(waitFor);
  }

  FutureAndPriority<T> addFirst<T>(
      Task<T> task, {
      dynamic key,
    }) {
    return _add(task, first: true, key: key);
  }

  Future<T> add<T>(
      Task<T> task, {
        int priority = 0,
        dynamic key,
      }) async {
    return _add(task, priority: priority, key: key).future;
  }

  FutureAndPriority<T> _add<T>(
    Task<T> task, {
      int priority = 0,
      bool first = false,
      dynamic key,
    }) {

    final c = Completer<T>();
    final job = () async {
      if (key != null) {
        _pendingKeys.add(key);
      }
      _pendingCount += 1;
      _intervalCount += 1;

      try {
        final operation = (_timeout == Duration.zero)
          ? task()
          : task().timeout(_timeout!,
            onTimeout: () {
              if (_throwOnTimeout) {
                throw TimeoutException('task timed out');
              }
              return null;
            } as FutureOr<T> Function()?
          );

        final result = await operation;
        emit(QueueEventAction.completed, result);
        c.complete(result);
      } on TimeoutException catch (error) {
        emit(QueueEventAction.error, error);
        c.completeError(error);
      } catch (error) {
        emit(QueueEventAction.error, error);
        c.completeError(error);
      }
      if (key != null) {
        _pendingKeys.remove(key);
      }
      _next();
    };

    int returnedPriority = priority;
    try {
      if (first) {
        returnedPriority = _queue.enqueueFirst(job, key: key);
      } else {
        _queue.enqueue(job, priority: priority, key: key);
      }
      emit(QueueEventAction.add);
    } catch (error) {
      emit(QueueEventAction.error, error);
      c.completeError(error);
    }

    _tryToStartAnother();

    return FutureAndPriority(c.future, returnedPriority);
  }

  RunFunction? remove(
    dynamic key,
  ) {
    final job = _queue.dequeue(key: key);
    emit(QueueEventAction.remove);
    return job;
  }

  int indexOf(dynamic key) {
    return _queue.indexOf(key);
  }

  ConcurrentQueue start() {

    if (!_isPaused) {
      return this;
    }
    _isPaused = false;

    _processQueue();

    return this;

  }

  void pause() {
    _isPaused = true;
  }

  void clear() {
    _queue = PriorityQueue();
  }

  Future<void> onEmpty() {
    Completer c = Completer();
    if (_queue.size == 0) {
      c.complete();
    } else {
      var existingResolve = _resolveEmpty;
      _resolveEmpty = () {
        existingResolve();
        c.complete();
      };
    }
    return c.future;
  }

  Future<void> onIdle() {
    Completer c = Completer();
    if (_pendingCount == 0 && _queue.size == 0) {
      c.complete();
    } else {
      var existingResolve = _resolveIdle;
      _resolveIdle = () {
        existingResolve();
        c.complete();
      };
    }

    return c.future;
  }

  int get size {
    return _queue.size;
  }

  int get pending {
    return _pendingCount;
  }

  bool get isPaused {
    return _isPaused;
  }

  Iterable<dynamic> get queuedOrPendingKeys => _queue.keys.followedBy(_pendingKeys);
  Iterable<dynamic> get pendingKeys => _pendingKeys;
  Iterable<dynamic> get queuedKeys => _queue.keys;
}
