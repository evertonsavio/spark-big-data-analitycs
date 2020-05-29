### "Numbers Everyone Should Know"
  
Some Latency Numbers:  
1. CPU - 0.4 nanoseconds  
2. Memory - 100 nanoseconds  
3. SSD - 16 microseconds  
4. Network - 150 miliseconds  
---------------------------------------------------  
* CPU -> Twitter: 6000 tweets/s, cada um por volta de 200bytes  = 1.2 milhoes de bytes ou 1mb/s  
* 2.5GHz CPU -> 2.5 Bilhões operações por segundo. Cada operação tem um processamento X Bytes ~~ 8 bytes  
* Então usariamos 0.01% da capacidade da cpu, não é um problema.  
  
* Knowing that tweets create approximately 104 billion bytes of data per day, (6000 tweets / second) x (86400 seconds / day) x (200 bytes / tweet) = 104 billion bytes / day. How long would it take the 2.5 GigaHertz CPU to analyze a full day of tweets? (say that for each operation, the CPU processes 8 bytes of data) Ans: 5.2 seconds  
--------------------------------------------------
* Mas há um detalhe...  Na maior parte do tempo a CPU não esta processando dados.  
* A Memoria Leva 250X tempo que a CPU para encontrar o mesmo byte na memória.  
--------------------------------------------------
#### Processo de 1 hora de Tweets -> Memoria:30ms, SSD:0.5s, HD:4s. Network ~~30s. 
--------------------------------------------------
#### SPARK foi desenvolvido especificamente para otimizar o uso da memória, trabalhando com clusters de computadores conectados pela REDE, então é crucial otimizar a rede. Existe um TRADEOFF devido a uso da rede.    

### Então, quando usar usar Big Data? Conheça esses numeros:    
* CPU 200X mais rápida que memória.  
* Memória é 15x mais rápida que SSD.  
* SSD é usualmente 20x mais rápido que rede.   
----------------------------------------------------  
  


  
