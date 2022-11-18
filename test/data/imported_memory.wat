;; Based on https://github.com/TooManyBees/wasm-demo/blob/master/imported_memory/imported_memory.wat
(module
  (memory (import "import" "memory") 1)
  (func (export "double") (param $ptr i32) (param $len i32)
    (local $end i32)
    (local.set $end
      (i32.mul
        (i32.const 4)
        (i32.add (local.get $ptr) (local.get $len))))

    (loop $loop
      (i32.store (local.get $ptr)
        (i32.mul (i32.load (local.get $ptr)) (i32.const 2)))
      (br_if $loop
        (i32.lt_s
          (local.tee $ptr (i32.add (local.get $ptr) (i32.const 4)))
          (local.get $end)))
    )
))
