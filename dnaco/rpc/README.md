## FRAME
Frames are in the form of | length (u32) | ...data... |
- the first 5bit are used to identify the protocol version (0-31).
- the following 27bit are used to identify the packet length (max 128M)
```
+-------+--------------------------------------+
| 11111 | 111 | 11111111 | 11111111 | 11111111 |
+-------+--------------------------------------+
0 rev.  5             data length             32
```

### Encryption/Signature
If frame are sent over an insecure transport encryption can be applied. 
The first 8bit are used to describe the algorithm used. The assumption here is that we used something like RSA + AES, so we have the aes key encrypted with the RSA key and the signature.
```
+----------+ +----------------+ +-----------+
| 11111111 | | encryption key | | signature |
+----------+ +----------------+ +-----------+
0          8      (N bytes)       (N bytes)
```

## RPC Header packets are composed of:
- *Packet Type*: 2bit (REQUEST, RESPONSE, EVENT, CONTROL)
- *Trace Id length*: 3bit (1 + (0-7)) max 8bytes int
- *Packet Id length*: 3bit (1 + (0-7)) max 8bytes int
```
+----+-----+-----+ +----------+ +-----------+
| 11 | 111 | 111 | | Trace Id | | Packet Id |
+----+-----+-----+ +----------+ +-----------+
0    2     5     8
```

### RPC Request Header packets are composed of:
 - *Operation Type*: 2bit (READ, WRITE, RW, COMPUTE)
 - *Send Result to*: 2bit (CALLER, STORE_IN_MEMORY, STORE_WITH_ID, FORWARD_TO)
 - *Request Id Length*: 6bit (1 + (0-63)) max 64bytes string
 - *Result Id Length*: 6bit (1 + (0-63)) max 64bytes string. used only when 
```
+----+----+--------+-------+ +------------+ +------------+
| 11 | 11 | 111111 | 11111 | | Request Id | | Result Id  |
+----+----+--------+-------+ +------------+ +------------+
0    2    4       10      16  (1-64 bytes)   (1-64 bytes)
```


### RPC Response Header Packets are composed of:
 - *Operation Status*: 2bit (SUCCEEDED, FAILED, CANCELLED, _)
 - *Queue Time length*: 3bit (1 + (0-7)) max 8bytes int
 - *Exec Time length*: 3bit (1 + (0-7)) max 8bytes int
```
+----+-----+-----+ +---------------+ +---------------+
| 11 | 111 | 111 | | Queue Time ns | | Exec Time ns  |
+----+-----+-----+ +---------------+ +---------------+
0    2     5     8    (1-8 bytes)       (1-8 bytes)
```
