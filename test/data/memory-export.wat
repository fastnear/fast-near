;; Borrowed from https://github.com/mvolkmann/wasm-memory-export/blob/master/demo.wat
(module
  ;; Import JS function to print a single i32 value to the console.
  (import "js" "log" (func $log (param i32)))

  (memory (export "myMemory") 1)

  (func $translate (param $offset i32) (param $delta f64)
    (f64.store
      (local.get $offset)
      (f64.add
        (f64.load (local.get $offset))
        (local.get $delta)
      )
    )
  )

  (func (export "translatePoints") (param $length i32) (param $dx f64) (param $dy f64)
    (local $offset i32) ;; starts at zero

    (local $lastOffset i32)
    (local.set $lastOffset
      (i32.mul
        (local.get $length) ;; number of points
        (i32.const 16) ;; 8 bytes for x + 8 bytes for y
      )
    )

    (loop
      (call $translate (local.get $offset) (local.get $dx))

      ;; Advance $offset to get next y value.
      (local.set $offset (i32.add (local.get $offset) (i32.const 8)))

      ;; Translate y value by $dy.
      (f64.store
        (local.get $offset)
        (f64.add
          (f64.load (local.get $offset))
          (local.get $dy)
        )
      )

      ;; Advance $offset to get next x value.
      (local.set $offset (i32.add (local.get $offset) (i32.const 8)))

      (br_if 0 (i32.lt_s (local.get $offset) (local.get $lastOffset)))
    )
  )
)
