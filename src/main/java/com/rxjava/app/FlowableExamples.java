package com.rxjava.app;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import io.reactivex.BackpressureStrategy;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.Flowable;
import io.reactivex.FlowableEmitter;
import io.reactivex.schedulers.Schedulers;

public class FlowableExamples {

    /**
     * A simple example of @{@link Flowable} with back-pressure
     * Back-Pressure comes by default to a Flowable
     * Flowable certainly adds some overhead as compared to Observable due to Back-Pressure.
     * Flowable obtained from Flowable.interval() factory is the only Flowable which does not provide Back-Pressure
     * because it is time bound.
     * @throws InterruptedException
     */
    @Test
    public void executeFlowableWithBackPressure () throws InterruptedException {
        // Creating a Flowable to emmit values from 1 to 999999999
        Flowable<Integer> flowable = Flowable.range(1, 999_999_999);

        flowable.
                map(data -> {
                    System.out.println("Processing value - " + data);
                    return data;
                }).
                // Running the Flowable on IO Thread
                observeOn(Schedulers.io()).
                subscribe(data -> {
                    System.out.println("Value received - " + data);
                    Thread.sleep(500);
                });

    }

    /**
     * Unlike Observable which takes an Observer, a Flowable takes a Subscriber. When creating a Subscriber from
     * scratch we will have to take care of Back-Pressure boundary limits.
     */
    @Test
    public void executeFlowableWithCustomSubscriberAndBackPressure () {

        // Creating a custom Subscriber
        Subscriber<Integer> customSubscriber = new Subscriber<Integer>() {

            AtomicInteger count = new AtomicInteger();
            Subscription subscription = null;

            @Override public void onSubscribe(Subscription subscription) {
                this.subscription = subscription;

                // This is where we control the Back-Pressure bounding limits of custom Subscribers
                // The Flowable / Producer will only release the number of items mentioned through Subscription.request
                // When the Subscriber is done consuming them the Subscriber will have to ask for the next batch
                // from the Flowable using the same api.
                subscription.request(80);
            }

            @Override public void onNext(Integer value) {
                System.out.println("Value received - " + value);

                if (count.incrementAndGet() % 60 == 0) {
                    // When the Subscriber has consumed 60 of the 80 items obtained it ask's for the next batch of
                    // 60 items there by always keeping a buffer of 20

                    // Note: Subscription.request defines the boundary rate between the Subscriber and the most nearest
                    // Operator in our case 'map' operator. The boundary rate between the 'map' operator and the
                    // Flowable will remain a predefined boundary value.

                    // Note: Commenting Subscription.request here will restrict the Subscriber to receive any further
                    // items beyond the first 80 items received in the beginning.
                    subscription.request(60);
                }

                sleep(500);
            }

            @Override public void onError(Throwable t) {

            }

            @Override public void onComplete() {
                System.out.println("Stream is over!!");
            }
        };

        // Creating a Flowable and assigning a Custom Subscriber to it.
        Flowable.range(1, 999).
        map(value -> {
            System.out.println("Publishing value - " + value);
            return value;
        }).
        observeOn(Schedulers.io()).
        subscribe(customSubscriber);
    }

    /**
     * When creating a Custom Flowable we need to take care of Back-Pressure mechanism, unlike creating a Custom
     * Observable. There are two forms of Custom Flowable:
     * 1. Custom Flowable with FAKE Back-Pressure mechanism (Buffering, Drop etc)
     * 2. Custom Flowable with REAL Back-Pressure mechanism
     * Here we will look into creating a Custom Flowable with Fake Back-Pressure mechanism specifically Buffering
     */
    @Test
    public void executeCustomFlowableWithFakeBackPressureStrategy () {
        Flowable.create(
            emitter -> {
                IntStream.range(1, 1000).forEach(
                    eachValue -> {
                        if (emitter.isCancelled())
                            return;
                        System.out.println("Emitting value - " + eachValue);
                        emitter.onNext(eachValue);
                    }
                );
                emitter.onComplete();
            },
            // Mentioning the Fake Back-Pressure strategy while creating the Custom Flowable
            BackpressureStrategy.BUFFER
        )
        .map(eachItem -> {
            System.out.println("Publishing value - " + eachItem);
            return eachItem;
        })
        .observeOn(Schedulers.io())
        .subscribe(eachItem -> {
            sleep(1000);
            System.out.println("Consuming Item - " + eachItem);
        });
    }

    /**
     * We can apply Back-Pressure to a Custom Flowable created with a Fake Back-Pressure strategy of
     * @{@link FlowableEmitter.BackpressureMode}::NONE at a later stage using onBackPressureXXX methods on Flowable
     */
    @Test
    public void applyBackPressureAtLatterStageOnFlowable () {
        // Creating a Custom Flowable with No Back-Pressure strategy
        Flowable<Integer> customFlowableWithNoBP = Flowable.create(
            emitter -> {
                IntStream.range(1, 1000).forEach(eachValue -> {
                    if (emitter.isCancelled())
                        return;
                    emitter.onNext(eachValue);
                });
                emitter.onComplete();
            },
            // Mentioning NO Back-Pressure Strategy
            BackpressureStrategy.MISSING
        );

        // Converting a NO BP Flowable to BP Flowable
        Flowable<Integer> customFlowableWithBP = customFlowableWithNoBP.onBackpressureBuffer(10000);

        customFlowableWithBP
            .observeOn(Schedulers.io())
            .subscribe(System.out::println);
    }

    /**
     * When creating a Custom Flowable with REAL Back-Pressure mechanism, we need to use the 'generate' method and pass
     * a State Object (if needed) to maintain a state across multiple invocation of Emitter::onNext. The Back-Pressure
     * is maintained automatically based on the Subscription's::request call (or what ever stage is right down below).
     */
    @Test
    public void executeCustomFlowableWithRealBackPressureStrategy () {

        /**
         * Creating a POJO to hold some STATE and will be used as a state container while creating the Flowable
         */
        class State {
            private int count;
            private Random randomNumberGenerator;

            State (int count) {
                this.count = count;
                this.randomNumberGenerator = new Random();
            }

            public synchronized boolean isValid () {
                if (--count > 0)
                    return true;
                else
                    return false;
            }

            public synchronized String getValue () {
                String value = "Value " + count + " - " + randomNumberGenerator.nextInt();
                System.out.println("Pushing --> " + value);
                return value;
            }

        }

        Flowable.generate(
            // Passing a Callable which in turn return's some form of a STATE object to be shared between multiple
            // invocation of onNext of the Emitter
            () -> new State(1000),
            // Passing a Bi-Function which will be called every time when there is a need to push new items, the
            // Flowable otherwise will automatically control the Back-Pressure. Each time the Bi-Function is called it
            // will be provided with the STATE object and the EMITTER
            (state, emitter) -> {
                if (state.isValid())
                    emitter.onNext(state.getValue());
                else
                    emitter.onComplete();
            }
        )
        .observeOn(Schedulers.io())
        .subscribe(value ->{
            System.out.println("Received --> " + value);
            sleep(1000);
        });
    }

    /**
     * Rather than using the 'generate' method as mentioned above to create a Custom Real Flowable with BP, we can
     * transform a @{@link Iterable} (either Custom or Provided) to a Real Flowable with BP using Flowable::fromIterable
     * method. If we create a Custom Iterable we can hold the State if required through CLOSURE while returning the
     * Iterator object from the Iterable, giving us the same flexibility as passing a State and Bi-Function to the
     * 'generate' method
     */
    @Test
    public void transformIterableToCustomFlowable () {

        class CustomIterable implements Iterable<Integer> {
            // We can hold global state for multiple Iterator here too.
            List<Integer> values;

            CustomIterable (List<Integer> values) {
                this.values = values;
            }

            @Override public Iterator<Integer> iterator() {
                // Holding a state for each iterator to be returned
                Iterator<Integer> iterator = values.iterator();
                return new Iterator<Integer>() {
                    @Override public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override public Integer next() {
                        return iterator.next();
                    }
                };
            }
        }

        CustomIterable customIterable = new CustomIterable(Arrays.asList(10, 20, 30, 40, 50));

        Flowable.fromIterable(customIterable)
        .observeOn(Schedulers.io())
        .subscribe(System.out::println);
    }

    public static void sleep (int sleepTime) {
        try {Thread.sleep(sleepTime);} catch (InterruptedException e) {throw new RuntimeException(e);}
    }
}
