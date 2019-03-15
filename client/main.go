package main



import (

    "log"

    "sync"

    "strconv"

    "os/exec"

    "strings"



    // "flag"

    // "fmt"

    // "os"



    context "golang.org/x/net/context"

    "google.golang.org/grpc"



    "github.com/nyu-distributed-systems-fa18/lab-2-raft-HotGiardiniera/pb"

)



func batch(endpoint string) {

    log.Printf("Connecting to %v", endpoint)

    // Connect to the server. We use WithInsecure since we do not configure https in this class.

    conn, err := grpc.Dial(endpoint, grpc.WithInsecure())

    //Ensure connection did not fail.

    if err != nil {

        log.Printf("ERROR Failed to dial GRPC server %v", err)

        return

    }

    log.Printf("Connected")

    // Create a KvStore client

    kvc := pb.NewKvStoreClient(conn)







    // Clear KVC

    res, err := kvc.Clear(context.Background(), &pb.Empty{})

    if err != nil {

        log.Printf("ERROR Could not clear")

        return

    }

    // REDIRECT

    for res.GetRedirect() != nil {

        redirect := string(res.GetRedirect().Server[4]) // eg. peer1:3000

        endpoint = getPeerIp(redirect)

        log.Printf("Redirecting to %v", endpoint)

        conn, err := grpc.Dial(endpoint, grpc.WithInsecure())

        if err != nil {

            log.Printf("ERROR Failed to dial GRPC server %v", err)

            return

        }

        log.Printf("Connected")

        kvc = pb.NewKvStoreClient(conn)

        res, err = kvc.Clear(context.Background(), &pb.Empty{})

        if err != nil {

            log.Printf("ERROR Could not clear")

            return

        }

    }









    // Put setting hello -> 1

    putReq := &pb.KeyValue{Key: "hello", Value: "1"}

    res, err = kvc.Set(context.Background(), putReq)

    if err != nil {

        log.Printf("ERROR Put error")

        return

    }

    log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)

    if res.GetKv().Key != "hello" || res.GetKv().Value != "1" {

        log.Printf("ERROR Put returned the wrong response")

        return

    }

    // REDIRECT

    for res.GetRedirect() != nil {

        redirect := string(res.GetRedirect().Server[4]) // eg. peer1:3000

        endpoint = getPeerIp(redirect)

        log.Printf("Redirecting to %v", endpoint)

        conn, err := grpc.Dial(endpoint, grpc.WithInsecure())

        if err != nil {

            log.Printf("ERROR Failed to dial GRPC server %v", err)

            return

        }

        log.Printf("Connected")

        kvc = pb.NewKvStoreClient(conn)

        putReq := &pb.KeyValue{Key: "hello", Value: "1"}

        res, err = kvc.Set(context.Background(), putReq)

        if err != nil {

            log.Printf("ERROR Put error")

            return

        }

        log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)

        if res.GetKv().Key != "hello" || res.GetKv().Value != "1" {

            log.Printf("ERROR Put returned the wrong response")

            return

        }

    }









    // Request value for hello

    req := &pb.Key{Key: "hello"}

    res, err = kvc.Get(context.Background(), req)

    if err != nil {

        log.Printf("ERROR Request error %v", err)

        return

    }

    // REDIRECT

    for res.GetRedirect() != nil {

        redirect := string(res.GetRedirect().Server[4]) // eg. peer1:3000

        endpoint = getPeerIp(redirect)

        log.Printf("Redirecting to %v", endpoint)

        conn, err := grpc.Dial(endpoint, grpc.WithInsecure())

        if err != nil {

            log.Printf("ERROR Failed to dial GRPC server %v", err)

            return

        }

        log.Printf("Connected")

        kvc = pb.NewKvStoreClient(conn)

        req := &pb.Key{Key: "hello"}

        res, err = kvc.Get(context.Background(), req)

        if err != nil {

            log.Printf("ERROR Request error %v", err)

            return

        }

    }

    log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)









    // Successfully CAS changing hello -> 2

    casReq := &pb.CASArg{Kv: &pb.KeyValue{Key: "hello", Value: "1"}, Value: &pb.Value{Value: "2"}}

    res, err = kvc.CAS(context.Background(), casReq)

    if err != nil {

        log.Printf("ERROR Request error %v", err)

        return

    }

    // REDIRECT

    for res.GetRedirect() != nil {

        redirect := string(res.GetRedirect().Server[4]) // eg. peer1:3000

        endpoint = getPeerIp(redirect)

        log.Printf("Redirecting to %v", endpoint)

        conn, err := grpc.Dial(endpoint, grpc.WithInsecure())

        if err != nil {

            log.Printf("ERROR Failed to dial GRPC server %v", err)

            return

        }

        log.Printf("Connected")

        kvc = pb.NewKvStoreClient(conn)

        casReq := &pb.CASArg{Kv: &pb.KeyValue{Key: "hello", Value: "1"}, Value: &pb.Value{Value: "2"}}

        res, err = kvc.CAS(context.Background(), casReq)

        if err != nil {

            log.Printf("ERROR Request error %v", err)

            return

        }

    }

    log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)









    // Unsuccessfully CAS

    casReq = &pb.CASArg{Kv: &pb.KeyValue{Key: "hello", Value: "1"}, Value: &pb.Value{Value: "3"}}

    res, err = kvc.CAS(context.Background(), casReq)

    if err != nil {

        log.Printf("ERROR Request error %v", err)

        return

    }

    // REDIRECT

    for res.GetRedirect() != nil {

        redirect := string(res.GetRedirect().Server[4]) // eg. peer1:3000

        endpoint = getPeerIp(redirect)

        log.Printf("Redirecting to %v", endpoint)

        conn, err := grpc.Dial(endpoint, grpc.WithInsecure())

        if err != nil {

            log.Printf("ERROR Failed to dial GRPC server %v", err)

            return

        }

        log.Printf("Connected")

        kvc = pb.NewKvStoreClient(conn)

        casReq = &pb.CASArg{Kv: &pb.KeyValue{Key: "hello", Value: "1"}, Value: &pb.Value{Value: "3"}}

        res, err = kvc.CAS(context.Background(), casReq)

        if err != nil {

            log.Printf("ERROR Request error %v", err)

            return

        }

    }

    log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)









    // CAS should fail for uninitialized variables

    casReq = &pb.CASArg{Kv: &pb.KeyValue{Key: "hellooo", Value: "1"}, Value: &pb.Value{Value: "2"}}

    res, err = kvc.CAS(context.Background(), casReq)

    if err != nil {

        log.Printf("ERROR Request error %v", err)

        return

    }

    // REDIRECT

    for res.GetRedirect() != nil {

        redirect := string(res.GetRedirect().Server[4]) // eg. peer1:3000

        endpoint = getPeerIp(redirect)

        log.Printf("Redirecting to %v", endpoint)

        conn, err := grpc.Dial(endpoint, grpc.WithInsecure())

        if err != nil {

            log.Printf("ERROR Failed to dial GRPC server %v", err)

            return

        }

        log.Printf("Connected")

        kvc = pb.NewKvStoreClient(conn)

        casReq = &pb.CASArg{Kv: &pb.KeyValue{Key: "hellooo", Value: "1"}, Value: &pb.Value{Value: "2"}}

        res, err = kvc.CAS(context.Background(), casReq)

        if err != nil {

            log.Printf("ERROR Request error %v", err)

            return

        }

    }

    log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)

}



func getPeerIp(peer string) string {

    ip_raw, _ := exec.Command("../launch-tool/launch.py", "client-url", peer).Output()

    ip := strings.TrimSpace(string(ip_raw))

    return ip

}



func main() {

    var wg sync.WaitGroup

    calls := 1000

    wg.Add(calls)

    num_peers := 5

    for i := 0; i < calls/num_peers; i++ {

        for j := 0; j < num_peers; j++ {

            ip := getPeerIp(strconv.Itoa(j))

            go func(e string) {

                defer wg.Done()

                batch(e)

            }(ip)

        }

    }

    wg.Wait()

}