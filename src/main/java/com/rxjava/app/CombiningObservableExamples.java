package com.rxjava.app;

import io.reactivex.Observable;
import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;
import lombok.AllArgsConstructor;
import lombok.ToString;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CombiningObservableExamples {

    /**
     * 'merge' merges two or multiple Observables<T> of type <T> and return an Observable<T>. When the merged Observable
     * is subscribed on, all the individual Observables are executed in Order and values are down-streamed from the
     * merged Observable (as long as all the individual Observables are running on a Single Thread)
     */
    @Test
    public void mergeOnSingleThread () {
        Observable<String> observable_1 = Observable.fromArray(1, 2, 3, 4, 5)
            .map(value -> "Observer_1 -> " + value);

        Observable<String> observable_2 = Observable.fromArray(6, 7, 8, 9, 10)
            .map(value -> "Observer_2 -> " + value);

        Observable.merge(observable_1, observable_2)
            .subscribe(System.out::println);
    }

    /**
     * When the individual Observables are scheduled to run on separate Thread, then all the individual Observables are
     * executed in an Un-Ordered or Out Of Order fashion (elements from single individual Observable is still in Order).
     */
    @Test
    public void mergeOnSeperateThread () {
        Observable<String> observable_1 = Observable.fromArray(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)
            .map(value -> "Observer_1 -> " + value + ", " + getCurrentThreadDetails())
            .subscribeOn(Schedulers.io());

        Observable<String> observable_2 = Observable.fromArray(16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30)
            .map(value -> "Observer_2 -> " + value + ", " + getCurrentThreadDetails())
            .subscribeOn(Schedulers.io());

        Observable.merge(observable_1, observable_2)
            .observeOn(Schedulers.computation())
            .subscribe(System.out::println);

        sleep(Integer.MAX_VALUE);
    }

    /**
     * 'flatMap' can transforms each streamed item in to an array of items of similar type as that of the streamed item
     * or that of a different type and streams each of the new array items individually to the downstream operators.
     * 'flatMap' takes a function which takes one argument of the streamed item type and returns an Observable wrapping
     * the array containing the new items to be streamed down-streams. As new items are received by flatMap it performs
     * a 'merge' of the newly created Observable with all the previously created Observables (for previous items received)
     * Hence items streamed down-stream from a flatMap could be Un-Ordered (based on 'merge' example above)
     *
     * 'flatMap' has an overloaded method which takes a concurrency limit, which is otherwise set to MAX. The concurrency
     * limit helps flatMap to process upstream items in parallel, which is unique as otherwise all other operators
     * execute on a single provided Thread. Hence with the concurrency limit we can contain how many upstream elements
     * can the flatMap execute and merge in parallel.
     *
     * NOTE: The concurrency mentioned above does not come by default to flatMap, by default flatMap is still single
     * threaded and sequential. Only when we schedule the Observable created with iin flatMap on a separate thread and
     * perform downstream operations directly within 'flatMap' only then we get concurrency.
     *
     * 'concatMap' is just like 'flatMap' minus the parallelism.
     */
    @Test
    public void executeFlatMap () {
        Observable<String> originalObservable = Observable.fromArray("Ford", "General Motors", "Honda", "Mazda");

        originalObservable
            .flatMap(
                eachStr ->
                    Observable.fromIterable(Arrays.asList(eachStr.split("")))
                        .observeOn(Schedulers.io())
                        .map(eachValue -> eachValue + " : " + getCurrentThreadDetails())
            )
            .blockingSubscribe(System.out::println);

        //sleep(Integer.MAX_VALUE);
    }

    /**
     * 'concat' operator is just like 'merge' except that it GUARANTEES that Observables will be executed In-Order even
     * when the individual Observables are scheduled to run on separate Thread, even then all the individual Observables
     * are executed in an In-Order fashion.
     */
    @Test
    public void executeConcatOperator () {
        Observable<String> observable_1 = Observable.fromArray(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)
            .map(value -> "Observer_1 -> " + value + ", " + getCurrentThreadDetails())
            .subscribeOn(Schedulers.io());

        Observable<String> observable_2 = Observable.fromArray(16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30)
            .map(value -> "Observer_2 -> " + value + ", " + getCurrentThreadDetails())
            .subscribeOn(Schedulers.io());

        Observable.concat(observable_1, observable_2)
            .observeOn(Schedulers.computation())
            .subscribe(System.out::println);

        sleep(Integer.MAX_VALUE);
    }

    /**
     * 'amb' variant operator takes n number of Observables, but 'amb' runs only one of them which emits the first
     * element and it drops the rest of the Observables. This is helpful when you have multiple sources for the same
     * data or events and you want the fastest one to win
     */
    @Test
    public void executeAmbOperator () {
        Observable<String> observable_1 = Observable.fromArray(1, 2, 3, 4, 5)
            .delay(10, TimeUnit.MILLISECONDS)
            .map(value -> "Observer_1 -> " + value);

        Observable<String> observable_2 = Observable.fromArray(6, 7, 8, 9, 10)
            .map(value -> "Observer_2 -> " + value);

        Observable.ambArray(observable_1, observable_2)
            .subscribe(System.out::println);
    }

    /**
     * 'zip' operator taken n number of Observables and a function whose input arguments take the output of each of
     * those Observables and streams a single emission for every collective emissions of all those Observables. An
     * emission from one Observable must wait to be paired with other emissions from other Observables before a single
     * emission can me streamed to down-stream operators from 'zip'. When we assign a Scheduler to each Observable none
     * of the Observable's actually waits for other Observables for their current emission to be paired before moving on
     * to streaming their next emission, rather emissions from fast streaming Observables are kept in memory so as when
     * emissions from all other Observables are received they could be paired.
     *
     * If an Observable return's lesser number of emissions from other Observables, the emissions from those other
     * Observable's are simply dropped
     */
    @Test
    public void executeZipOperatorWithParallelism () {
        Observable<String> observable_1 = Observable.fromArray(1, 2, 3, 4, 5)
            .delay(1000, TimeUnit.MILLISECONDS)
            .map(
                value -> {
                    return "Obsv_1 : " + getCurrentThreadDetails() + " : " + value + " : " + System.currentTimeMillis();
                }
            )
            .subscribeOn(Schedulers.io());

        Observable<String> observable_2 = Observable.fromArray(6, 7, 8, 9, 10)
            .map(
                value -> {
                    return "Obsv_2 : " + getCurrentThreadDetails() + " : " + value + " : " + System.currentTimeMillis();
                }
            )
            .subscribeOn(Schedulers.io());

        Observable.zip(
            observable_1
            ,observable_2
            ,(value_1, value_2) ->
                getCurrentThreadDetails() + "{ " + value_1 + " } { " + value_2 + " }"

        )
            .subscribe(System.out::println);

        sleep(Integer.MAX_VALUE);
    }

    /**
     * 'combineLatest' operator taken n number of Observables and a function whose input arguments take the output of
     * each of those Observables and streams a single emission for every collective MOST RECENT emissions of all those
     * Observables. An emission from one Observable does not get queued in memory to be paired with other emissions from
     * other Observables before a single emission can me streamed to down-stream operators from 'combineLatest'. Rather
     * a the MOST RECENT emission is always kept in memory for every Observable so that when new emissions from all
     * Observables are received it can pair the MOST RECENT emission from every Observable and stream a single emission
     * downstream.
     *
     * If an Observable return's lesser number of emissions from other Observables, the LAST emission of that Observable
     * is used to pair with those extra emissions from those other Observable's but they are not dropped.
     */
    @Test
    public void executeCombineLatestOperatorWithParallelism () {
        Observable<String> observable_1 = Observable.fromArray(1, 2, 3, 4, 5)
            .delay(1000, TimeUnit.MILLISECONDS)
            .map(
                value -> {
                    return "Obsv_1 : " + getCurrentThreadDetails() + " : " + value + " : " + System.currentTimeMillis();
                }
            )
            .subscribeOn(Schedulers.io());

        Observable<String> observable_2 = Observable.fromArray(6, 7, 8, 9, 10)
            .map(
                value -> {
                    return "Obsv_2 : " + getCurrentThreadDetails() + " : " + value + " : " + System.currentTimeMillis();
                }
            )
            .subscribeOn(Schedulers.io());

        Observable.combineLatest(
            observable_1
            ,observable_2
            ,(value_1, value_2) ->
                getCurrentThreadDetails() + "{ " + value_1 + " } { " + value_2 + " }"

        )
            .subscribe(System.out::println);

        sleep(Integer.MAX_VALUE);
    }

    /**
     * 'groupBy' is almost like 'toMultiMap'. It accumulates all emission and emits 'n' number of emissions each
     * represented as Observable<K, V>, here:
     * n - total number of groups or accumulated emissions
     * K - is the key corresponding to each emission
     * V - is kind of the actual Observable representing all emissions corresponding to the paired Key 'K'
     *
     * Unlike 'toMultiMap' which makes a single emission representing the MAP<K, LIST<V>>.
     *
     * We can easily apply a 'flatMap' below 'groupBy' to perform parallel consumption of each key and it's values. This
     * is not possible in the case of 'toMultiMap' as it makes a single emission representing the whole
     * MAP<K, LIST<V>>
     */
    @Test
    public void executeGroupByOperator () {

        // A model class representing each streamed element
        class Person {
            public String name;
            public int rollNumber;

            public Person (String name, int rollNumber) {
                this.name = name;
                this.rollNumber = rollNumber;
            }

            @Override public String toString() {
                return "Person{" + "name='" + name + '\'' + ", rollNumber=" + rollNumber + '}';
            }
        }

        // Generating a list of Persons with duplicate rollNumbers
        List<Integer> rollNumbers = Arrays.asList(1, 2, 2, 4, 6, 4);
        List<Person> personsWithDuplicateRollNumber = IntStream.range(0, rollNumbers.size())
            .mapToObj(index -> new Person("person-"+index, rollNumbers.get(index)))
            .collect(Collectors.toList());

        Observable.fromIterable(personsWithDuplicateRollNumber)
            .groupBy(person -> person.rollNumber)
            .flatMapSingle(groupedObservable ->
                    groupedObservable
                        .doOnNext(person ->
                            System.out.println("Key - " + groupedObservable.getKey() + ", "
                                + person.toString() + ", " + getCurrentThreadDetails())
                        )
                        .toList()
                        .subscribeOn(Schedulers.computation())
            )
            .blockingSubscribe(System.out::println);

    }

    public void sleep (int sleepTime) {
        try {Thread.sleep(sleepTime);} catch (Exception e) {}
    }

    public String getCurrentThreadDetails () {
        return Thread.currentThread().getName() + "-" + Thread.currentThread().getId();
    }
}
