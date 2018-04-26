package com.rxjava.app;

import io.reactivex.Observable;
import io.reactivex.Observer;
import io.reactivex.disposables.Disposable;
import io.reactivex.observables.ConnectableObservable;
import io.reactivex.schedulers.Schedulers;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ObservableExamples {

    /**
     * An example of Cold Observable where the complete stream is replayed for every Observer
     * Using a Custom Observable and Custom Observer
     */
    @Test
    public void executeCustomColdObservableAndObserver() {
        List<String> data = Arrays.asList("A", "B", "C", "D");

        Observer<String> customObserver = new Observer<String>() {
            @Override public void onSubscribe(Disposable d) {
                System.out.println("The observer subscribed");
            }

            @Override public void onNext(String value) {
                System.out.println("Custom Observer - " + value);
            }

            @Override public void onError(Throwable e) {
                System.out.println("An error occurred - " + e.getMessage());
            }

            @Override public void onComplete() {
                System.out.println("Data stream is over");
            }
        };

        Observable<String> customObservable = Observable.create(emitter -> {

            // iterating over a list of String (data) and calling onNext to stream each data
            // to be received by the end subscriber
            data.forEach(eachValue -> {
                emitter.onNext(eachValue);
            });

            // This will invoke onError on the end subscriber
            //emitter.onError(new RuntimeException("Some Error"));

            // This will invoke the onComplete on the end subscriber
            emitter.onComplete();
        });

        customObservable.
            map(eachStreamData -> eachStreamData.toLowerCase()) // applying an operator
            // Subscribing to the Stream
            // Observable takes Observer or Consumer and supports throttling
            // Flowable takes Subscriber and supports back-pressure
            .subscribe(customObserver);

        // An example of COLD Observable where the complete stream is replayed for every Observer
        customObservable.
            subscribe(eachValue -> {
                System.out.println("Observer 2 - " + eachValue);
            });
    }

    /**
     * An example of HOT Observable where the complete stream is NOT replayed for every Observer.
     */
    @Test
    public void executeHotObservable () {
        // This Observable will run on a separate thread (Computation Scheduler)
        // and emmit an incremental value every time period.
        Observable<Long> coldObservable = Observable.interval(500, TimeUnit.MILLISECONDS);

        // Transforming a COLD Observable to a HOT Observable
        ConnectableObservable<Long> hotObservable = coldObservable.publish();
        // this is where the HOT-Observable will start publishing / emitting values
        hotObservable.connect();

        // Subscribing the first observer/subscriber to the HOT-Observable
        hotObservable.subscribe(data -> {
            System.out.println("Subscriber 1 - " + data);
        });

        // waiting for some time before we subscribe the second subscriber/observer
        sleep(4000);

        // Subscribing the second observer/subscriber to the HOT-Observable
        // Unlike in the case of COLD-Observable the HOT-Observable will not replay the data already
        // published for the second subscriber
        hotObservable.subscribe(data -> {
            System.out.println("Subscriber 2 - " + data);
        });
    }

    /**
     * Back-Pressure is NOT provided by default to COLD-Observables (or any Observable)
     * when the Observer and the Observable are running on different Thread-Pools
     * for Back-Pressure look into @{@link io.reactivex.Flowable}
     * RxJava 1.0 had Back-Pressure enabled on Observable by default but that changed with RxJava 2.0 where two
     * different entities (Flowable & observable) were created and BP came default only with Flowable. Primarily
     * because applying BP is expensive and hence did not make sense to be applied over all Observables.
     */
    @Test
    public void failToExecuteBackPressureOverColdObservables () {
        // Creating a COLD-Observable
        // Observable<Integer> coldObservable = Observable.range(1, 10000);
        Observable<String> coldObservable = Observable.create(emitter -> {
            for (long count = 0; count < Long.MAX_VALUE; count++) {
                if (count%1000000 == 0)
                    System.out.println("Observable publishing - " + count);
                emitter.onNext("{publisher_thread: " +
                    Thread.currentThread().getName() +
                    ", value: " +
                    count +
                    "}");
            }
            emitter.onComplete();
        });

        // Running the COLD-Observable on a separate thread scheduler (Compute)
        coldObservable.observeOn(Schedulers.computation()).

            // Running the Observer on a separate thread scheduler (IO)
            subscribeOn(Schedulers.io()).

            // Back-Pressure DOES NOT come inbuilt for COLD-Observables (or any Observables)
            // Here the Observable is running on a Computation thread pool
            // And the Observer is running on a IO thread pool (running slower because of the sleep)
            // As the Observable is running faster than the Observer and there is NO inbuilt Back-Pressure built into
            // Observables the memory continues to grow and then fails at certain point by throwing an exception.
            subscribe(data -> {
                sleep(1000);
                System.out.println("receiver_thread - " + Thread.currentThread().getName() + ", Data - " + data);
            });

    }

    /**
     * Back-Pressure comes into play by default in COLD-Observables
     * when both Observer and Observable are running on the same Thread. Mostly because it's a PULL architecture
     * between the Observer and Observable
     */
    @Test
    public void executeBackPressureOverColdObservables () {
        // Creating a COLD-Observable
        // Observable<Integer> coldObservable = Observable.range(1, 10000);
        Observable<String> coldObservable = Observable.create(emitter -> {
            for (long count = 0; count < Long.MAX_VALUE; count++) {
                if (count%1000000 == 0)
                    System.out.println("Observable publishing - " + count);
                emitter.onNext("{publisher_thread: " +
                    Thread.currentThread().getName() +
                    ", value: " +
                    count +
                    "}");
            }
            emitter.onComplete();
        });


        // Back-Pressure kicks in for COLD-Observables when both the Observer and Observable are running
        // on the same Thread due to the PULL architecture between Observer and Observable
        coldObservable.subscribe(data -> {
            sleep(1000);
            System.out.println("receiver_thread - " + Thread.currentThread().getName() + ", Data - " + data);
        });

    }

    /**
     * Throttling, Buffering etc can also be done on COLD-Observable.
     */
    @Test
    public void executeThrottlingOverColdObservable () {
        // Creating a COLD-Observable
        Observable<String> coldObservable = Observable.create(emitter -> {
            for (int count = 0; count < 10000; count++) {
                Thread.sleep(5);
                emitter.onNext("{publisher_thread: " +
                    Thread.currentThread().getName() +
                    ", value: " +
                    count +
                    "}");
            }
            emitter.onComplete();
        });

        // Throttling the Observable
        coldObservable.throttleFirst(100, TimeUnit.MILLISECONDS).

            // Running the COLD-Observable on a separate thread scheduler (Compute)
                observeOn(Schedulers.computation()).

            // Running the Observer on a separate thread scheduler (IO)
                subscribeOn(Schedulers.io()).


            subscribe(data -> {
                sleep(10);
                System.out.println("receiver_thread - " + Thread.currentThread().getName() + ", Data - " + data);
            }, error -> {
                System.out.println("Error - " + error.getMessage());
            });

    }

    /**
     * Hot Observable Buffering Example
     */
    @Test
    public void executeHotObservableBufferingExample () {
        // Creating a buffered COLD-Observable with Buffering capability
        Observable<List<Long>> coldObservable = Observable.interval(100, TimeUnit.MILLISECONDS)
            .buffer(1000, TimeUnit.MILLISECONDS);

        // Creating a HOT-Buffered-Observable from the COLD-Buffered-Observable
        ConnectableObservable<List<Long>> hotObservable = coldObservable.publish();
        // Start publishing values from HOT-Buffered-Observable
        hotObservable.connect();

        // Subscribing the first observer/subscriber to the HOT-Buffered-Observable
        hotObservable.subscribe(data -> {
            System.out.println("Observer 1 - " + data);
            Thread.sleep(800);
        });

        // waiting for some time before we subscribe the second subscriber/observer
        sleep(4000);

        // Subscribing the second observer/subscriber to the HOT-Buffered-Observable
        hotObservable.subscribe(data -> {
            System.out.println("Observer 2 - " + data);
            Thread.sleep(800);
        });

    }

    /**
     * Hot Observable Throttling Example
     */
    @Test
    public void executeHotObservableThrottlingExample () {
        // Creating a throttled COLD-Observable
        Observable<Long> coldObservable = Observable.interval(100, TimeUnit.MILLISECONDS)
            .throttleLast(1000, TimeUnit.MILLISECONDS);

        // Creating a HOT-Throttled-Observable from the COLD-Buffered-Observable
        ConnectableObservable<Long> hotObservable = coldObservable.publish();
        // Start publishing values from HOT-Throttled-Observable
        hotObservable.connect();

        // Subscribing the first observer/subscriber to the HOT-Throttled-Observable
        hotObservable.subscribe(data -> {
            System.out.println("Observer 1 - " + data);
            sleep(800);
        });

        // waiting for some time before we subscribe the second subscriber/observer
        sleep(4000);

        // Subscribing the second observer/subscriber to the HOT-Throttled-Observable
        hotObservable.subscribe(data -> {
            System.out.println("Observer 2 - " + data);
            sleep(800);
        });

    }

    public static void sleep(int sleepTime) {
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
