# Masai

Masai is a very simple interface to a number of key-value stores. It leverages Clojure's protocols and datatypes to make pluggable backends easy. Masai is meant to be a uniform and simple interface to pluggable backends, and not a comprehensive interface to all of them.

Currently supported by masai are Tokyo Cabinent and Redis. Memcached support is in the works.

# Usage

First of all, decide what backend you want to use. Install whatever is necessary and get that running. After that, you'll want to `use` whatever backend you're using and masai's protocol namespace, `masai.db`. For example, if you decided to use the redis backend:

    (ns my.ns
      (:use masai.db masai.redis))

Now you need to create an instance of DB to work with. Every backend has a `make` function for doing this.

    (def db (make))

This will create a default instance of it for you. This assumes that your installation of whatever key-value store the backend corresponds to is default as well. If you're running redis on a non-default port, this wont work. You can specify the port by passing `:port newport` to `make`.

Some backends, such as tokyo, require that you run the `open` function before the database will be usable. In those cases, do this:

    (open db)

Now you can work with your db. You can add stuff:

    (add! db "foo" (.getBytes "bar"))

Get stuff:

    (get "db" "foo")

And even delete stuff:

    (delete! db "foo")
