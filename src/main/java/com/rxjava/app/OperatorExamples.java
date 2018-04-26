package com.rxjava.app;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.reactivex.schedulers.Schedulers;
import org.junit.Test;

import io.reactivex.Observable;

public class OperatorExamples {

    /**
     * 'filter' operator filter's the stream and only let go specific values to the downstream subscriber's
     */
    @Test
    public void executeFilterOperator () {
        Observable.range(1, 100)
            .filter(value -> (value % 2) == 0)
            .subscribe(System.out::println);
    }

    /**
     * 'take' operator only passes a specific number of stream values to it's downstream subscribers either from the
     * beginning of the stream or the end of the stream depending on which 'take' method was invoked.
     */
    @Test
    public void executeTakeOperator () {
        Observable.range(1, 20)
            .take(10)
            .subscribe(System.out::println);
    }

    /**
     * 'skip' operator skip's a specified number of stream values either from the beginning or end of the stream
     * depending on which 'skip' method was invoked and the rest are passed down to the downstream subscriber's
     */
    @Test
    public void executeSkipOperator () {
        Observable.range(1, 20)
            .skip(10)
            .subscribe(System.out::println);
    }

    /**
     * 'distinct' help's in obtaining the distinct values (where the distinctness is obtained either by comparing the
     * values directly or by comparing some property of the value). 'distinct' leads to growth in memory as the library
     * holds Unique values in memory to perform the distinct check on future streams.
     */
    @Test
    public void executeDistinctOperator () {
        Observable<String> observable = Observable.fromArray("demand", "strategy", "hollow", "market", "machine", "demand");

        observable
            // This operator find's the distinct of all the values in the stream and remove all the duplicates
            .distinct()
            .subscribe(System.out::println);

        System.out.println(" ======================================================================== ");

        observable
            // Here we are finding distinct based on some property of the values (here two values are duplicate if their
            // LENGTH is same) and passing those unique values down to the downstream subscriber
            .distinct(value -> value.length())
            .subscribe(System.out::println);
    }

    /**
     * 'distinctUntilChanged' ignores duplicate consecutive emissions
     */
    @Test
    public void executeDistinctUntilChangedOperator () {
        Observable<Integer> observable = Observable.fromArray(1, 1, 1, 2, 2, 3, 3, 3, 1, 1);

        observable
            // The operator ignores duplicate consecutive emissions
            .distinctUntilChanged()
            .subscribe(System.out::println);

        Observable<String> observable2 = Observable.fromArray("One", "Two", "Three", "Four", "Five");

        observable2
            // Here we are finding consecutive distinct based on some property of the values
            .distinctUntilChanged(value -> value.length())
            .subscribe(System.out::println);
    }

    /**
     * The 'map' operator transforms an emission of T to R
     */
    @Test
    public void executeMapOperator () {
        Observable.fromArray(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
            .map(value -> "{" + value + "}")
            .subscribe(System.out::println);
    }

    /**
     * 'startWith' operator sneaks in a single or multiple elements to the beginning of the stream
     */
    @Test
    public void executeStartWithOperator () {
        Observable.fromArray("A", "B", "C")
            .startWith("Menu:")
            .subscribe(System.out::println);
    }

    /**
     * 'defaultEmpty' operator returns a default value if the stream fails to return even a single value.
     */
    @Test
    public void executeDefaultIfEmpty () {
        Observable.fromArray("One", "Two", "Three")
            .filter(value -> value.length() < 2)
            .defaultIfEmpty("No valid number")
            .subscribe(System.out::println);
    }

    /**
     * 'switchIfEmpty' operator switches to another provided Observable if the stream fails to return even a single
     * value.
     */
    @Test
    public void executeSwitchIfEmpty () {
        Observable.fromArray("One", "Two", "Three")
            .filter(value -> value.length() < 2)
            .switchIfEmpty(Observable.fromArray("Four", "Five"))
            .subscribe(System.out::println);
    }

    /**
     * 'sorted' operator sorts a stream. To do that it certainly has to collect all the stream elements first, hence
     * has to wait till the end of the stream and then performs the sort and pushes it downstream. Clearly this will
     * grow the memory
     */
    @Test
    public void executeSortedOperator () {
        Observable.fromArray(10, 4, 7)
            .sorted()
            .subscribe(System.out::println);
    }

    /**
     * 'scan' operator takes a Bi-Function, where the first parameter is the output of the Bi-Function for the previous
     * streamed element and the second parameter is the current streamed element. Each out of the function is also
     * pushed downstream
     */
    @Test
    public void executeScanOperator () {
        Observable.fromArray(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
            .scan((total, eachValue) -> total + eachValue)
            .subscribe(System.out::println);
    }

    /**
     * 'scan' operator takes an initial value and a Bi-Function, where the output type of the Bi-Function should
     * be same as that of the initial value and does not have to be the same as that of the streamed element type. Hence
     * we can even pass a memory container (like a list) as the initial value which will be passed across to every
     * invocation and behave as a STATE. Scan will pass down each accumulated STATE to the downstream subscribers but
     * Reduce will pass down only the last accumulated STATE.
     *
     * Note: passing an immutable state can have bad effects and hence should be avoided, rather this is particularly
     * used to pass an initial value / seed
     */
    @Test
    public void executeScanWithInitialValueOperator () {
        Observable.fromArray(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
            .scan(new ArrayList<Integer>(), (list, value) -> {list.add(value); return list;})
            .subscribe(System.out::println);
    }

    /**
     * counts the number of emission
     */
    @Test
    public void executeCountOperator () {
        Observable.fromArray(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
            .count()
            .subscribe(System.out::println);
    }

    /**
     * 'reduce' operator takes a Bi-Function, where the first parameter is the output of the Bi-Function for the previous
     * streamed element and the second parameter is the current streamed element. Only the last accumulated output is
     * pushed downstream
     */
    @Test
    public void executeReduceOperator () {
        Observable.fromArray(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
            .reduce((total, eachValue) -> total + eachValue)
            .subscribe(System.out::println);
    }

    /**
     * 'reduce' operator takes an initial value and a Bi-Function, where the output type of the Bi-Function should
     * be same as that of the initial value and does not have to be the same as that of the streamed element type. Hence
     * we can even pass a memory container (like a list) as the initial value which will be passed across to every
     * invocation and behave as a STATE. Reduce will pass down he last accumulated STATE to the downstream subscribers
     *
     * Note: passing an immutable state can have bad effects and hence should be avoided, rather this is particularly
     * used to pass an initial value / seed
     */
    @Test
    public void executeReduceWithInitialValueOperator () {
        Observable.fromArray(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
            .reduce(new ArrayList<Integer>(), (list, value) -> {list.add(value); return list;})
            .subscribe(System.out::println);
    }

    /**
     * 'all' operator pushes downstream a single boolean value marking whether all the elements upstream qualifies to a
     * specific condition
     */
    @Test
    public void executeAllOperator () {
        Observable.fromArray(1, 2, 3, 4, 5, 10)
            .all(value -> value < 10)
            .subscribe(System.out::println);
    }

    /**
     * As compared to 'all' operator 'any' just checks if at-least one upstream value matches a provided condition, and
     * pushes a single boolean value representing that result to the downstream.
     */
    @Test
    public void executeAnyOperator () {
        Observable.fromArray(1, 2, 3, 4, 5, 10)
            .any(value -> value == 10)
            .subscribe(System.out::println);
    }


    /**
     * Based on the equals and hashcode the 'contains' method checks if at-least one upstream value 'equals' a provided
     * value, and pushes a single boolean value representing that result to the downstream.
     */
    @Test
    public void executeContainsOperator () {
        Observable.fromArray(1, 2, 3, 4, 5, 10)
            .contains(10)
            .subscribe(System.out::println);
    }

    /**
     * Collection operators will accumulate all emissions into a collection such as a list or map and then emit that
     * entire collection as a single emission. Memory will grow and seems like an anti-stream property
     */
    @Test
    public void executeToListOperator () {
        Observable.fromArray(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
            .toList()
            .subscribe(System.out::println);
    }

    /**
     * 'toMap' operator will accumulate all emissions into a MAP and then emit that entire MAP as a single emission.
     * Memory will grow and seems like an anti-stream property.
     */
    @Test
    public void executeToMapOperator () {

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

        // Generating a list of Persons
        List<Person> persons = Arrays.asList(1, 2, 3, 4, 5)
            .stream()
            .map(value -> new Person("person-"+value, value))
            .collect(Collectors.toList());

        // Creating a Observable out of the List of Person's
        Observable<Person> personObservable = Observable.fromIterable(persons);

        /**
         * For a stream of type <T> the 'toMap' operator takes ONE function to generate the 'KEY' <K> for a particular
         * streamed element, where the value remains the same streamed element of type <T>. Hence for a stream T it
         * generates a MAP<K,T>
         */
        personObservable
            .toMap(person -> person.rollNumber)
            .subscribe(System.out::println);

        /**
         * For a stream of type <T> this version of 'toMap' operator takes TWO functions. The first function generates a
         * KEY (K) for a particular streamed element. And the second function is a transformation function transforming
         * the streamed type <T> to <M>. Hence for a stream T it generates a MAP<K,M>
         */
        personObservable
            .toMap(person -> person.rollNumber, person -> person.name)
            .subscribe(System.out::println);

        // Generating a list of Persons with duplicate rollNumbers
        List<Integer> rollNumbers = Arrays.asList(1, 2, 2, 4, 6, 4);
        List<Person> personsWithDuplicateRollNumber = IntStream.range(0, rollNumbers.size())
            .mapToObj(index -> new Person("person-"+index, rollNumbers.get(index)))
            .collect(Collectors.toList());

        /**
         * For a stream of type <T> 'toMultimap' generates a MAP<K, LIST<T>> or MAP<K, LIST<M>>. It primarily groups all
         * items with a similar key into a list and returns a MAP<K, LIST<T>>, it also has a variant which takes a
         * transformation function and hence returns MAP<K, LIST<M>> where the transformer transforms a type T to M.
         */
        Observable.fromIterable(personsWithDuplicateRollNumber)
            .toMultimap(person -> person.rollNumber, person -> person.name)
            .subscribe(System.out::println);

    }

    /**
     * 'collect' operators will accumulate all emissions into a collection of our choice and then emit that
     * entire collection as a single emission. It takes teo function the first one should create and return a
     * Collection object of our choice and the second on add's streamed values to that collection.
     */
    @Test
    public void executeCollectOperator () {
        Observable.fromArray(1, 2, 2, 4, 4, 6, 7, 8, 8, 10)
            .collect(HashSet<Integer>::new, HashSet<Integer>::add)
            .subscribe(System.out::println);
    }

    /**
     * 'onErrorReturnXXX' operators returns a default value in case an exception occurs. But unlike onError the
     * positioning of the 'onErrorReturnXXX' matters, it can only catch upstream exceptions and it won't catch
     * downstream exceptions. Other elements beyonf the position where the exception occurred won't be streamed.
     */
    @Test
    public void executeOnErrorReturnOperator () {

        Observable.fromArray(1, 2, 3)
            .map(value -> {
                    if (value == 2)
                        throw new RuntimeException("Error from first map");
                    return value;
                }
            )
            .onErrorReturnItem(200)
            .subscribe(System.out::println, e -> System.out.println(e.getMessage()));

        Observable.fromArray(1, 2, 3)
            .onErrorReturnItem(200)
            .map(value -> {
                    if (value == 3)
                        throw new RuntimeException("Error from second map");
                    return value;
                }
            )
            .subscribe(System.out::println, e -> System.out.println(e.getMessage()));
    }

    /**
     * Unlike other operators which takes a Function(<T>):<T> or Function(<T>):<M> for an Observable<T>. 'to' operator
     * takes a Function(Observable<T>):Observable<T> or Function(Observable<T>):Observable<M>. Primarily it is used to
     * apply some external behavior provided by some external function to the current chain, where the output of that
     * external function should also be an Observable so that downstream operators of the current chain could be applied
     */
    @Test
    public void executeToOperator () {
        Observable.fromArray(1, 2, 3)
            .to(value -> value
                .map(v -> v + 10)
                .toList()
            )
            .subscribe(System.out::println);
    }

    public void sleep (int sleepTime) {
        try {Thread.sleep(sleepTime);} catch (InterruptedException e) {throw new RuntimeException(e);}
    }

    public String getCurrentThreadDetails () {
        return Thread.currentThread().getName() + "-" + Thread.currentThread().getId();
    }
}
